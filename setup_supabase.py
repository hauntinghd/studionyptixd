"""
One-time Supabase setup script.
Creates profiles table and seeds the admin + pro accounts.

Run: python setup_supabase.py

Requires SUPABASE_URL and SUPABASE_SERVICE_KEY (service_role key from Supabase dashboard).
The service_role key has admin powers -- never expose it to the frontend.
"""

import os
import sys
import json
import subprocess
from pathlib import Path

try:
    import httpx
except ImportError:
    print("Installing httpx...")
    subprocess.run([sys.executable, "-m", "pip", "install", "httpx"], check=True)
    import httpx

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())

REQUIRED_ENV_VARS = (
    "SUPABASE_URL",
    "SUPABASE_SERVICE_KEY",
    "SUPABASE_ADMIN_EMAIL",
    "SUPABASE_ADMIN_PASSWORD",
    "SUPABASE_PRO_EMAIL",
    "SUPABASE_PRO_PASSWORD",
)


def _required_configuration() -> dict[str, str]:
    config = {name: str(os.getenv(name, "") or "").strip() for name in REQUIRED_ENV_VARS}
    missing = [name for name, value in config.items() if not value]
    if missing:
        # Report names only. Never echo credential values or offer an interactive
        # paste path that can leak into a terminal transcript.
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
    return config


def main():
    config = _required_configuration()
    supabase_url = config["SUPABASE_URL"].rstrip("/")
    service_key = config["SUPABASE_SERVICE_KEY"]
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
    }
    accounts = [
        {
            "label": "admin",
            "email": config["SUPABASE_ADMIN_EMAIL"],
            "password": config["SUPABASE_ADMIN_PASSWORD"],
            "plan": "admin",
            "role": "admin",
        },
        {
            "label": "pro",
            "email": config["SUPABASE_PRO_EMAIL"],
            "password": config["SUPABASE_PRO_PASSWORD"],
            "plan": "pro",
            "role": "user",
        },
    ]
    client = httpx.Client(timeout=30)

    print("\n=== STEP 1: Create profiles table via SQL ===")
    sql = """
    CREATE TABLE IF NOT EXISTS public.profiles (
        id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
        plan TEXT NOT NULL DEFAULT 'free',
        role TEXT NOT NULL DEFAULT 'user',
        stripe_customer_id TEXT,
        stripe_subscription_id TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Users can read own profile') THEN
            CREATE POLICY "Users can read own profile" ON public.profiles
                FOR SELECT USING (auth.uid() = id);
        END IF;
    END $$;

    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'Service role full access') THEN
            CREATE POLICY "Service role full access" ON public.profiles
                FOR ALL USING (auth.role() = 'service_role');
        END IF;
    END $$;

    CREATE OR REPLACE FUNCTION public.handle_new_user()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO public.profiles (id, plan, role)
        VALUES (NEW.id, 'free', 'user')
        ON CONFLICT (id) DO NOTHING;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql SECURITY DEFINER;

    DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
    CREATE TRIGGER on_auth_user_created
        AFTER INSERT ON auth.users
        FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();
    """

    resp = client.post(
        f"{supabase_url}/rest/v1/rpc/",
        headers={**headers, "Prefer": ""},
        content=sql,
    )
    if resp.status_code >= 400:
        print(f"  SQL via RPC may not work (status {resp.status_code}). Trying pg_net...")
        print("  You may need to run the SQL manually in the Supabase SQL Editor.")
        print("  The SQL is printed below.\n")
        print(sql)
        print("\n  Copy the above into Supabase Dashboard > SQL Editor > New query > Run")
        input("  Press Enter after running the SQL...")
    else:
        print("  Profiles table created!")

    print("\n=== STEP 2: Create user accounts ===")
    for acct in accounts:
        print(f"\n  Creating configured {acct['label']} account...")

        resp = client.get(
            f"{supabase_url}/auth/v1/admin/users?per_page=500",
            headers=headers,
        )
        existing_id = None
        if resp.status_code == 200:
            data = resp.json()
            users = data.get("users", data) if isinstance(data, dict) else data
            for u in users:
                if u.get("email") == acct["email"]:
                    existing_id = u["id"]
                    print("    Account already exists")
                    break

        if not existing_id:
            resp = client.post(
                f"{supabase_url}/auth/v1/admin/users",
                headers=headers,
                json={
                    "email": acct["email"],
                    "password": acct["password"],
                    "email_confirm": True,
                },
            )
            if resp.status_code in (200, 201):
                user_data = resp.json()
                existing_id = user_data.get("id")
                print("    Account created")
            else:
                print(f"    ERROR creating configured account (status {resp.status_code})")
                continue

        print(f"    Setting plan to '{acct['plan']}'...")
        resp = client.post(
            f"{supabase_url}/rest/v1/profiles",
            headers={**headers, "Prefer": "resolution=merge-duplicates"},
            json={"id": existing_id, "plan": acct["plan"], "role": acct["role"]},
        )
        if resp.status_code in (200, 201):
            print(f"    Plan set!")
        else:
            print(f"    Profile upsert failed (status {resp.status_code})")

    print("\n=== DONE ===")
    print("Configured admin and pro accounts are ready with confirmed emails.")

    client.close()


if __name__ == "__main__":
    main()

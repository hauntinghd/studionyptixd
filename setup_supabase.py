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
from pathlib import Path

try:
    import httpx
except ImportError:
    print("Installing httpx...")
    os.system(f"{sys.executable} -m pip install httpx")
    import httpx

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

if not SERVICE_KEY:
    print("\n*** SUPABASE_SERVICE_KEY not found in .env ***")
    print("Go to: Supabase Dashboard > Settings > API > service_role key")
    print("Add to .env:  SUPABASE_SERVICE_KEY=eyJ...")
    SERVICE_KEY = input("Or paste it here: ").strip()
    if not SERVICE_KEY:
        sys.exit(1)

if not SUPABASE_URL:
    print("SUPABASE_URL not set")
    sys.exit(1)

headers = {
    "apikey": SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type": "application/json",
}

ACCOUNTS = [
    {"email": "omatic657@gmail.com", "password": "TheCCAS111##", "plan": "admin", "role": "admin"},
    {"email": "alwakmyhem@gmail.com", "password": "TheCCAS113##", "plan": "pro", "role": "user"},
]


def main():
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
        f"{SUPABASE_URL}/rest/v1/rpc/",
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
    for acct in ACCOUNTS:
        print(f"\n  Creating {acct['email']}...")

        resp = client.get(
            f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
            headers=headers,
        )
        existing_id = None
        if resp.status_code == 200:
            data = resp.json()
            users = data.get("users", data) if isinstance(data, dict) else data
            for u in users:
                if u.get("email") == acct["email"]:
                    existing_id = u["id"]
                    print(f"    Already exists (id: {existing_id})")
                    break

        if not existing_id:
            resp = client.post(
                f"{SUPABASE_URL}/auth/v1/admin/users",
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
                print(f"    Created! (id: {existing_id})")
            else:
                print(f"    ERROR creating user: {resp.status_code} {resp.text}")
                continue

        print(f"    Setting plan to '{acct['plan']}'...")
        resp = client.post(
            f"{SUPABASE_URL}/rest/v1/profiles",
            headers={**headers, "Prefer": "resolution=merge-duplicates"},
            json={"id": existing_id, "plan": acct["plan"], "role": acct["role"]},
        )
        if resp.status_code in (200, 201):
            print(f"    Plan set!")
        else:
            print(f"    Profile upsert: {resp.status_code} {resp.text}")

    print("\n=== DONE ===")
    print("Admin:  omatic657@gmail.com  (full admin, all features)")
    print("Pro:    alwakmyhem@gmail.com  (pro plan, all features)")
    print("\nBoth accounts have confirmed emails (no verification needed).")

    client.close()


if __name__ == "__main__":
    main()

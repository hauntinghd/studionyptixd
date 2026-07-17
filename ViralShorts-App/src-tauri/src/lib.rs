use tauri::{plugin::TauriPlugin, Manager, Runtime, Url};

#[cfg(desktop)]
use tauri_plugin_deep_link::DeepLinkExt;

const STUDIO_AUTH_SCHEME: &str = "nyptid-studio";

fn is_internal_navigation(url: &Url) -> bool {
    if url.scheme() == "tauri" || url.as_str() == "about:blank" {
        return true;
    }

    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if host == "tauri.localhost" {
        return true;
    }

    cfg!(debug_assertions)
        && matches!(url.scheme(), "http" | "https")
        && matches!(host.as_str(), "127.0.0.1" | "localhost")
        && url.port_or_known_default() == Some(8080)
}

fn is_trusted_external_navigation(url: &Url) -> bool {
    if url.scheme() != "https" {
        return false;
    }

    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    matches!(
        host.as_str(),
        "accounts.google.com"
            | "qdwzilgqvpegekxrrnnn.supabase.co"
            | "checkout.stripe.com"
            | "billing.stripe.com"
            | "buy.stripe.com"
            | "paypal.com"
            | "www.paypal.com"
            | "www.sandbox.paypal.com"
            | "studio.nyptidindustries.com"
            | "nyptidindustries.com"
            | "www.nyptidindustries.com"
            | "invoicer.nyptidindustries.com"
            | "nyptid-studio.fly.dev"
    )
}

fn is_trusted_auth_deep_link(url: &Url) -> bool {
    url.scheme() == STUDIO_AUTH_SCHEME
        && url.host_str() == Some("auth")
        && url.path() == "/callback"
        && url.username().is_empty()
        && url.password().is_none()
        && url.port().is_none()
}

fn focus_main_window<R: Runtime>(app: &tauri::AppHandle<R>) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
    }
}

fn trusted_navigation<R: Runtime>() -> TauriPlugin<R> {
    tauri::plugin::Builder::new("trusted-navigation")
        .on_navigation(|_webview, url| {
            if is_internal_navigation(url) {
                return true;
            }

            if is_trusted_external_navigation(url) {
                if let Err(error) = tauri_plugin_opener::open_url(url.as_str(), None::<&str>) {
                    log::error!("failed to open trusted external URL: {error}");
                }
            } else {
                log::warn!("blocked untrusted top-level navigation to {url}");
            }
            false
        })
        .build()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let mut builder = tauri::Builder::default();

    // Windows and Linux deliver a desktop deep link by launching the registered
    // executable again. Register single-instance first so the already-running
    // Studio process (which owns the PKCE verifier) receives that callback.
    #[cfg(desktop)]
    {
        builder = builder.plugin(tauri_plugin_single_instance::init(|app, argv, _cwd| {
            let has_auth_callback = argv
                .iter()
                .filter_map(|arg| Url::parse(arg).ok())
                .any(|url| is_trusted_auth_deep_link(&url));
            if has_auth_callback {
                focus_main_window(app);
            }
        }));
    }

    builder
        .plugin(tauri_plugin_deep_link::init())
        .plugin(trusted_navigation())
        .setup(|app| {
            // Bundled installers register this statically. Runtime registration
            // also makes the portable Windows executable return OAuth callbacks
            // to whichever copy the beta tester actually launched.
            #[cfg(any(windows, target_os = "linux"))]
            if let Err(error) = app.deep_link().register_all() {
                // Installer registration remains authoritative. A locked-down
                // HKCU must not make Studio itself fail to start.
                log::warn!("could not register portable deep links: {error}");
            }

            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running NYPTID Studio");
}

#[cfg(test)]
mod tests {
    use super::{
        is_internal_navigation, is_trusted_auth_deep_link, is_trusted_external_navigation,
    };
    use tauri::Url;

    #[test]
    fn only_known_https_checkout_and_auth_hosts_are_externalized() {
        assert!(is_trusted_external_navigation(
            &Url::parse("https://checkout.stripe.com/c/pay/example").unwrap()
        ));
        assert!(is_trusted_external_navigation(
            &Url::parse("https://accounts.google.com/o/oauth2/v2/auth").unwrap()
        ));
        assert!(!is_trusted_external_navigation(
            &Url::parse("https://checkout.stripe.com.attacker.example/phish").unwrap()
        ));
        assert!(!is_trusted_external_navigation(
            &Url::parse("http://checkout.stripe.com/c/pay/example").unwrap()
        ));
    }

    #[test]
    fn bundled_app_origin_is_internal() {
        assert!(is_internal_navigation(
            &Url::parse("tauri://localhost/?page=dashboard").unwrap()
        ));
        assert!(is_internal_navigation(
            &Url::parse("http://tauri.localhost/?page=dashboard").unwrap()
        ));
    }

    #[test]
    fn desktop_auth_callback_requires_exact_scheme_host_and_path() {
        assert!(is_trusted_auth_deep_link(
            &Url::parse("nyptid-studio://auth/callback?code=one-time-code").unwrap()
        ));
        assert!(!is_trusted_auth_deep_link(
            &Url::parse("nyptid-studio://attacker/callback?code=one-time-code").unwrap()
        ));
        assert!(!is_trusted_auth_deep_link(
            &Url::parse("nyptid-studio://auth/other?code=one-time-code").unwrap()
        ));
        assert!(!is_trusted_auth_deep_link(
            &Url::parse("https://auth/callback?code=one-time-code").unwrap()
        ));
    }
}

use tauri::{plugin::TauriPlugin, Emitter, Manager, Runtime, Url};

#[cfg(desktop)]
use tauri_plugin_deep_link::DeepLinkExt;

const STUDIO_AUTH_SCHEME: &str = "nyptid-studio";
const STUDIO_WEB_APP_HOST: &str = "studio.nyptidindustries.com";
#[cfg(not(debug_assertions))]
const STUDIO_WEB_APP_URL: &str =
    "https://studio.nyptidindustries.com/?desktop=1&page=dashboard&tab=agent";

fn is_studio_web_app_navigation(url: &Url) -> bool {
    url.scheme() == "https"
        && url.host_str() == Some(STUDIO_WEB_APP_HOST)
        && url.port().is_none()
        && url.username().is_empty()
        && url.password().is_none()
}

fn is_internal_navigation(url: &Url) -> bool {
    if url.scheme() == "tauri" || url.as_str() == "about:blank" {
        return true;
    }

    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if host == "tauri.localhost" {
        return true;
    }

    // The desktop app is an evergreen, least-privilege shell around the same
    // production frontend as Studio Web. Exact-origin matching prevents a
    // lookalike subdomain from ever receiving Tauri IPC access.
    if is_studio_web_app_navigation(url) {
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

#[cfg(windows)]
fn apply_studio_windows_frame(window: &tauri::WebviewWindow) {
    use std::{ffi::c_void, mem::size_of};
    use windows::Win32::Graphics::Dwm::{
        DwmSetWindowAttribute, DWMWA_BORDER_COLOR, DWMWA_CAPTION_COLOR, DWMWA_TEXT_COLOR,
    };

    let Ok(hwnd) = window.hwnd() else {
        return;
    };
    let black: u32 = 0x000000;
    let white: u32 = 0x00ff_ffff;
    let apply = |attribute, color: &u32| unsafe {
        DwmSetWindowAttribute(
            hwnd,
            attribute,
            color as *const u32 as *const c_void,
            size_of::<u32>() as u32,
        )
    };
    if let Err(error) = apply(DWMWA_BORDER_COLOR, &black) {
        log::warn!("could not set Studio window border color: {error}");
    }
    if let Err(error) = apply(DWMWA_CAPTION_COLOR, &black) {
        log::warn!("could not set Studio window caption color: {error}");
    }
    if let Err(error) = apply(DWMWA_TEXT_COLOR, &white) {
        log::warn!("could not set Studio window title color: {error}");
    }
}

fn trusted_navigation<R: Runtime>() -> TauriPlugin<R> {
    tauri::plugin::Builder::new("trusted-navigation")
        .on_navigation(|_webview, url| {
            if is_internal_navigation(url) {
                return true;
            }

            if is_trusted_external_navigation(url) {
                // ShellExecute can block on some Windows/browser combinations.
                // Never run it on Tauri's WebView navigation callback thread.
                let external_url = url.to_string();
                std::thread::spawn(move || {
                    if let Err(error) =
                        tauri_plugin_opener::open_url(external_url, None::<&str>)
                    {
                        log::error!("failed to open trusted external URL: {error}");
                    }
                });
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
            let auth_callbacks: Vec<String> = argv
                .iter()
                .filter_map(|arg| Url::parse(arg).ok())
                .filter(|url| is_trusted_auth_deep_link(url))
                .map(|url| url.to_string())
                .collect();
            if !auth_callbacks.is_empty() {
                focus_main_window(app);
                // Explicitly forward the second-instance command line to the
                // live web UI. The deep-link plugin still handles cold starts;
                // the event guarantees warm callbacks reach the PKCE owner.
                if let Err(error) = app.emit("nyptid-desktop-auth-urls", auth_callbacks) {
                    log::error!("could not forward desktop auth callback: {error}");
                }
            }
        }));
    }

    builder
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_process::init())
        .plugin(trusted_navigation())
        .setup(|app| {
            #[cfg(windows)]
            if let Some(window) = app.get_webview_window("main") {
                apply_studio_windows_frame(&window);
            }

            // Release builds use the live Studio frontend as the source of
            // truth, so web and desktop never drift apart again. Development
            // builds retain the local Vite server for normal iteration.
            #[cfg(not(debug_assertions))]
            if let Some(window) = app.get_webview_window("main") {
                let live_url = Url::parse(STUDIO_WEB_APP_URL)?;
                window.navigate(live_url)?;
            }

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
        is_internal_navigation, is_studio_web_app_navigation, is_trusted_auth_deep_link,
        is_trusted_external_navigation,
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
    fn only_exact_studio_web_origin_is_internal() {
        assert!(is_studio_web_app_navigation(
            &Url::parse("https://studio.nyptidindustries.com/?desktop=1").unwrap()
        ));
        assert!(is_internal_navigation(
            &Url::parse("https://studio.nyptidindustries.com/?page=subscription").unwrap()
        ));
        assert!(!is_studio_web_app_navigation(
            &Url::parse("https://studio.nyptidindustries.com.attacker.example/").unwrap()
        ));
        assert!(!is_studio_web_app_navigation(
            &Url::parse("http://studio.nyptidindustries.com/").unwrap()
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

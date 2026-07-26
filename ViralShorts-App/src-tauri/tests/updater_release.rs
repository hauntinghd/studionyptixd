use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use minisign_verify::{PublicKey, Signature};
use serde_json::Value;
use std::{env, fs, path::PathBuf};

fn updater_public_key() -> PublicKey {
    let config: Value = serde_json::from_str(include_str!("../tauri.conf.json"))
        .expect("tauri.conf.json must be valid JSON");
    let encoded = config["plugins"]["updater"]["pubkey"]
        .as_str()
        .expect("Tauri updater public key must be configured");
    let decoded = BASE64
        .decode(encoded)
        .expect("Tauri updater public key must be single-base64");
    let minisign_document =
        std::str::from_utf8(&decoded).expect("decoded updater public key must be UTF-8");
    PublicKey::decode(minisign_document)
        .expect("decoded updater public key must be a valid Minisign public key")
}

#[test]
fn updater_trust_anchor_uses_the_exact_tauri_minisign_format() {
    let _ = updater_public_key();
}

#[test]
fn supplied_release_artifact_verifies_like_the_tauri_updater() {
    let Some(artifact) = env::var_os("NYPTID_UPDATER_VERIFY_ARTIFACT").map(PathBuf::from) else {
        return;
    };
    let Some(signature) = env::var_os("NYPTID_UPDATER_VERIFY_SIGNATURE").map(PathBuf::from) else {
        panic!("NYPTID_UPDATER_VERIFY_SIGNATURE is required when an artifact is supplied");
    };

    let artifact_bytes = fs::read(&artifact).expect("could not read updater artifact");
    let encoded_signature =
        fs::read_to_string(&signature).expect("could not read updater signature");
    let decoded_signature = BASE64
        .decode(encoded_signature.trim())
        .expect("updater signature must be single-base64");
    let minisign_document =
        std::str::from_utf8(&decoded_signature).expect("decoded signature must be UTF-8");
    let signature =
        Signature::decode(minisign_document).expect("decoded signature must be valid Minisign");

    updater_public_key()
        .verify(&artifact_bytes, &signature, true)
        .expect("artifact signature must verify exactly as Tauri updater verifies it");
}

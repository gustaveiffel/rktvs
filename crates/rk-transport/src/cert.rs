// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

/// Generate a self-signed certificate for localhost (development/testing).
pub fn generate_self_signed() -> Result<(CertificateDer<'static>, PrivatePkcs8KeyDer<'static>), rcgen::Error> {
    generate_self_signed_for(vec!["localhost".to_string()])
}

/// Generate a self-signed certificate with custom Subject Alternative Names.
pub fn generate_self_signed_for(
    hostnames: Vec<String>,
) -> Result<(CertificateDer<'static>, PrivatePkcs8KeyDer<'static>), rcgen::Error> {
    let cert = rcgen::generate_simple_self_signed(hostnames)?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    Ok((cert_der, key_der))
}

/// Save a DER-encoded certificate to disk.
pub fn save_cert(cert: &CertificateDer<'_>, path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, cert.as_ref())
}

/// Save a DER-encoded private key to disk with restricted permissions (0o600).
///
/// On Unix, the file is created with mode 0o600 atomically via OpenOptions
/// to avoid a TOCTOU window where the key would be world-readable.
pub fn save_key(key: &PrivatePkcs8KeyDer<'_>, path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(key.secret_pkcs8_der())?;
    }
    #[cfg(not(unix))]
    {
        fs::write(path, key.secret_pkcs8_der())?;
    }
    Ok(())
}

/// Load a DER-encoded certificate from disk.
pub fn load_cert(path: &Path) -> io::Result<CertificateDer<'static>> {
    let bytes = fs::read(path)?;
    Ok(CertificateDer::from(bytes))
}

/// Load a DER-encoded private key from disk.
pub fn load_key(path: &Path) -> io::Result<PrivatePkcs8KeyDer<'static>> {
    let bytes = fs::read(path)?;
    Ok(PrivatePkcs8KeyDer::from(bytes))
}

/// Build a quinn ServerConfig from a certificate + key.
pub fn server_config(
    cert: CertificateDer<'static>,
    key: PrivatePkcs8KeyDer<'static>,
) -> Result<quinn::ServerConfig, rustls::Error> {
    let server_config = quinn::ServerConfig::with_single_cert(vec![cert], key.into())?;
    Ok(server_config)
}

/// Build a quinn ClientConfig that trusts a specific certificate.
pub fn client_config(cert: &CertificateDer<'static>) -> anyhow::Result<quinn::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.clone())?;
    let client_config = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    Ok(client_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_self_signed_and_build_configs() {
        let (cert, key) = generate_self_signed().unwrap();
        let server_config = server_config(cert.clone(), key).unwrap();
        let client_config = client_config(&cert).unwrap();
        assert!(std::mem::size_of_val(&server_config) > 0);
        assert!(std::mem::size_of_val(&client_config) > 0);
    }

    #[test]
    fn generate_with_custom_sans() {
        let (cert, key) = generate_self_signed_for(vec![
            "localhost".into(),
            "127.0.0.1".into(),
        ]).unwrap();
        // Should produce a valid cert+key that builds configs
        let _sc = server_config(cert.clone(), key).unwrap();
        let _cc = client_config(&cert).unwrap();
    }

    #[test]
    fn save_load_cert_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, _key) = generate_self_signed().unwrap();

        let cert_path = dir.path().join("hub.cert.der");
        save_cert(&cert, &cert_path).unwrap();
        let loaded = load_cert(&cert_path).unwrap();
        assert_eq!(cert.as_ref(), loaded.as_ref());
    }

    #[test]
    fn save_load_key_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (_cert, key) = generate_self_signed().unwrap();

        let key_path = dir.path().join("hub.key.der");
        save_key(&key, &key_path).unwrap();
        let loaded = load_key(&key_path).unwrap();
        assert_eq!(key.secret_pkcs8_der(), loaded.secret_pkcs8_der());

        // Check permissions on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = std::fs::metadata(&key_path).unwrap();
            assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn load_nonexistent_cert_errors() {
        let result = load_cert(Path::new("/nonexistent/cert.der"));
        assert!(result.is_err());
    }

    #[test]
    fn load_nonexistent_key_errors() {
        let result = load_key(Path::new("/nonexistent/key.der"));
        assert!(result.is_err());
    }

    #[test]
    fn save_load_full_roundtrip_configs() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = generate_self_signed().unwrap();

        save_cert(&cert, &dir.path().join("c.der")).unwrap();
        save_key(&key, &dir.path().join("k.der")).unwrap();

        let cert2 = load_cert(&dir.path().join("c.der")).unwrap();
        let key2 = load_key(&dir.path().join("k.der")).unwrap();

        // Must be able to build valid configs from loaded cert+key
        let _sc = server_config(cert2.clone(), key2).unwrap();
        let _cc = client_config(&cert2).unwrap();
    }
}

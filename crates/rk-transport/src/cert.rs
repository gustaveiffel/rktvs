use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

/// Generate a self-signed certificate for localhost (development/testing).
pub fn generate_self_signed() -> Result<(CertificateDer<'static>, PrivatePkcs8KeyDer<'static>), rcgen::Error> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    Ok((cert_der, key_der))
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
}

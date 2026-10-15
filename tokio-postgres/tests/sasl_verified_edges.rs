//! Edges of `Error::sasl_verified` beyond tests/sasl_verified.rs: cleartext password
//! authentication never sets it, and neither does a SASL offer the client can't use.

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio_postgres::config::SslMode;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Config, NoTls};

async fn read_startup(s: &mut DuplexStream) {
    let len = s.read_i32().await.unwrap();
    let mut body = vec![0; usize::try_from(len).unwrap() - 4];
    s.read_exact(&mut body).await.unwrap();
}

async fn read_msg(s: &mut DuplexStream) -> u8 {
    let tag = s.read_u8().await.unwrap();
    let len = s.read_i32().await.unwrap();
    let mut body = vec![0; usize::try_from(len).unwrap() - 4];
    s.read_exact(&mut body).await.unwrap();
    tag
}

async fn send(s: &mut DuplexStream, tag: u8, body: &[u8]) {
    let mut out = vec![tag];
    out.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
    out.extend_from_slice(body);
    s.write_all(&out).await.unwrap();
}

async fn error_28p01(s: &mut DuplexStream) {
    let mut body = Vec::new();
    for (field, value) in [
        (b'S', "FATAL"),
        (b'C', "28P01"),
        (b'M', "password authentication failed"),
    ] {
        body.push(field);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    send(s, b'E', &body).await;
}

fn config() -> Config {
    let mut config = Config::new();
    config
        .user("u")
        .password("pencil")
        .dbname("app")
        .ssl_mode(SslMode::Disable);
    config
}

#[tokio::test]
async fn a_cleartext_password_refusal_is_not_verified() {
    let (client_end, mut server_end) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(async move {
        read_startup(&mut server_end).await;
        send(&mut server_end, b'R', &3i32.to_be_bytes()).await;
        assert_eq!(read_msg(&mut server_end).await, b'p');
        error_28p01(&mut server_end).await;
    });
    let err = config()
        .connect_raw(client_end, NoTls)
        .await
        .map(|_| ())
        .unwrap_err();
    server.await.unwrap();
    assert_eq!(err.code(), Some(&SqlState::INVALID_PASSWORD), "{err:?}");
    assert!(!err.sasl_verified(), "{err:?}");
    assert!(
        !err.is_tls() && !err.is_channel_binding_refused(),
        "{err:?}"
    );
}

// A SASL offer with no mechanism the client knows: refused before any exchange.
#[tokio::test]
async fn an_unknown_sasl_mechanism_is_not_verified() {
    let (client_end, mut server_end) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(async move {
        read_startup(&mut server_end).await;
        let mut body = 10i32.to_be_bytes().to_vec();
        body.extend_from_slice(b"OAUTHBEARER\0\0");
        send(&mut server_end, b'R', &body).await;
        let mut buf = [0; 64];
        while matches!(server_end.read(&mut buf).await, Ok(n) if n > 0) {}
    });
    let err = config()
        .connect_raw(client_end, NoTls)
        .await
        .map(|_| ())
        .unwrap_err();
    server.await.unwrap();
    assert!(!err.sasl_verified(), "{err:?}");
    assert!(
        !err.is_tls() && !err.is_channel_binding_refused(),
        "{err:?}"
    );
}

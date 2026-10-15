//! What a failed `connect` says about how far the startup got, without a live server: a
//! scripted server on an in-memory stream drives the authentication exchange.
//!
//! - `Error::sasl_verified`: the server's SCRAM final message arrived and its server signature
//!   verified before this error (the server proved it knows the password; a refusal after it,
//!   such as Postgres's `clientcert=verify-full` check, isn't about the password).
//! - `Error::is_tls`: the error came from setting up TLS (the connector failed, or the server
//!   refused TLS that the configuration requires).
//! - `Error::is_channel_binding_refused`: channel binding was required and the server
//!   authenticated without it.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio_postgres::config::{ChannelBinding as ChannelBindingMode, SslMode};
use tokio_postgres::error::SqlState;
use tokio_postgres::tls::{ChannelBinding, TlsConnect, TlsStream};
use tokio_postgres::{Config, Error, NoTls};

const PASSWORD: &str = "pencil";
const SALT: &[u8] = b"salt-for-tests";
const ITERATIONS: u32 = 4096;
const SSL_REQUEST_CODE: i32 = 80_877_103;
const SCRAM: &str = "SCRAM-SHA-256";
const SCRAM_PLUS: &str = "SCRAM-SHA-256-PLUS";
/// What a fake TLS session reports as the server certificate's hash.
const END_POINT: &[u8] = b"server-cert-hash";

// ---- The scripted server ---------------------------------------------------------------

struct Server {
    s: DuplexStream,
}

impl Server {
    /// Reads the startup message (after answering an SSLRequest with `ssl`, when one comes).
    async fn startup(&mut self, ssl: Option<u8>) {
        if let Some(answer) = ssl {
            self.ssl_request(answer).await;
            if answer != b'S' {
                return;
            }
        }
        let len = self.s.read_i32().await.unwrap();
        let mut body = vec![0; usize::try_from(len).unwrap() - 4];
        self.s.read_exact(&mut body).await.unwrap();
    }

    /// Reads the SSLRequest and answers `answer` (`S` or `N`).
    async fn ssl_request(&mut self, answer: u8) {
        let len = self.s.read_i32().await.unwrap();
        let code = self.s.read_i32().await.unwrap();
        assert_eq!((len, code), (8, SSL_REQUEST_CODE), "an SSLRequest first");
        self.s.write_all(&[answer]).await.unwrap();
    }

    async fn read_msg(&mut self) -> (u8, Vec<u8>) {
        let tag = self.s.read_u8().await.unwrap();
        let len = self.s.read_i32().await.unwrap();
        let mut body = vec![0; usize::try_from(len).unwrap() - 4];
        self.s.read_exact(&mut body).await.unwrap();
        (tag, body)
    }

    async fn send(&mut self, tag: u8, body: &[u8]) {
        let mut out = vec![tag];
        out.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
        out.extend_from_slice(body);
        self.s.write_all(&out).await.unwrap();
    }

    async fn auth(&mut self, code: i32, data: &[u8]) {
        let mut body = code.to_be_bytes().to_vec();
        body.extend_from_slice(data);
        self.send(b'R', &body).await;
    }

    async fn error(&mut self, code: &str, message: &str) {
        let mut body = Vec::new();
        for (field, value) in [
            (b'S', "FATAL"),
            (b'V', "FATAL"),
            (b'C', code),
            (b'M', message),
        ] {
            body.push(field);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        self.send(b'E', &body).await;
    }

    async fn ready(&mut self) {
        let mut key = 42i32.to_be_bytes().to_vec();
        key.extend_from_slice(&7i32.to_be_bytes());
        self.send(b'K', &key).await;
        self.send(b'Z', b"I").await;
    }

    /// Runs SCRAM up to the server's final message: offers `mechanisms`, answers the client's
    /// first message, reads its final one. Returns the mechanism the client chose, its
    /// channel-binding data (`c=`, decoded) and the server signature a real server would send.
    async fn scram_until_final(&mut self, mechanisms: &[&str]) -> (String, Vec<u8>, [u8; 32]) {
        let mut offer = Vec::new();
        for m in mechanisms {
            offer.extend_from_slice(m.as_bytes());
            offer.push(0);
        }
        offer.push(0);
        self.auth(10, &offer).await;

        let (tag, body) = self.read_msg().await;
        assert_eq!(tag, b'p', "SASLInitialResponse");
        let nul = body.iter().position(|b| *b == 0).unwrap();
        let mechanism = String::from_utf8(body[..nul].to_vec()).unwrap();
        let client_first = String::from_utf8(body[nul + 5..].to_vec()).unwrap();
        let bare = &client_first[client_first.find(",,").unwrap() + 2..];
        let client_nonce = bare.split_once(",r=").unwrap().1;
        let server_first = format!(
            "r={client_nonce}server-nonce,s={},i={ITERATIONS}",
            STANDARD.encode(SALT)
        );
        self.auth(11, server_first.as_bytes()).await;

        let (tag, body) = self.read_msg().await;
        assert_eq!(tag, b'p', "SASLResponse");
        let client_final = String::from_utf8(body).unwrap();
        let without_proof = &client_final[..client_final.find(",p=").unwrap()];
        let binding = STANDARD
            .decode(
                without_proof
                    .strip_prefix("c=")
                    .unwrap()
                    .split(',')
                    .next()
                    .unwrap(),
            )
            .unwrap();
        let auth_message = format!("{bare},{server_first},{without_proof}");
        let salted = hi(PASSWORD.as_bytes(), SALT, ITERATIONS);
        let server_key = hmac(&salted, b"Server Key");
        (
            mechanism,
            binding,
            hmac(&server_key, auth_message.as_bytes()),
        )
    }

    /// `AuthenticationSASLFinal` with `signature`.
    async fn scram_final(&mut self, signature: &[u8; 32]) {
        let body = format!("v={}", STANDARD.encode(signature));
        self.auth(12, body.as_bytes()).await;
    }

    /// SCRAM-SHA-256 (no channel binding) through a final message that verifies.
    async fn verified_scram(&mut self) {
        let (mechanism, _, signature) = self.scram_until_final(&[SCRAM]).await;
        assert_eq!(mechanism, SCRAM);
        self.scram_final(&signature).await;
    }

    /// Holds the stream open until the client goes.
    async fn drain(&mut self) {
        let mut buf = [0; 1024];
        while matches!(self.s.read(&mut buf).await, Ok(n) if n > 0) {}
    }
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn hi(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut first = salt.to_vec();
    first.extend_from_slice(&[0, 0, 0, 1]);
    let mut prev = hmac(password, &first);
    let mut out = prev;
    for _ in 1..iterations {
        prev = hmac(password, &prev);
        for (o, p) in out.iter_mut().zip(prev) {
            *o ^= p;
        }
    }
    out
}

// ---- A TLS stand-in: a pass-through stream that reports channel-binding data -----------

#[derive(Clone)]
struct FakeTls {
    /// The handshake fails with this error.
    fail: Option<fn() -> io::Error>,
    /// `tls-server-end-point` data, or none.
    end_point: Option<Vec<u8>>,
}

impl FakeTls {
    fn ok() -> Self {
        FakeTls {
            fail: None,
            end_point: Some(END_POINT.to_vec()),
        }
    }
}

struct FakeTlsStream {
    inner: DuplexStream,
    end_point: Option<Vec<u8>>,
}

impl TlsConnect<DuplexStream> for FakeTls {
    type Stream = FakeTlsStream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<FakeTlsStream>> + Send>>;

    fn connect(self, inner: DuplexStream) -> Self::Future {
        Box::pin(async move {
            match self.fail {
                Some(fail) => Err(fail()),
                None => Ok(FakeTlsStream {
                    inner,
                    end_point: self.end_point,
                }),
            }
        })
    }
}

impl AsyncRead for FakeTlsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for FakeTlsStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl TlsStream for FakeTlsStream {
    fn channel_binding(&self) -> ChannelBinding {
        match &self.end_point {
            Some(data) => ChannelBinding::tls_server_end_point(data.clone()),
            None => ChannelBinding::none(),
        }
    }
}

// ---- Running a script -------------------------------------------------------------------

/// No TLS (so no SSLRequest, whatever the connector).
fn plain_config() -> Config {
    let mut config = Config::new();
    config
        .user("u")
        .password(PASSWORD)
        .dbname("app")
        .ssl_mode(SslMode::Disable);
    config
}

/// TLS required, channel binding required: how the orchestrator reaches a compute.
fn tls_config() -> Config {
    let mut config = plain_config();
    config
        .ssl_mode(SslMode::Require)
        .channel_binding(ChannelBindingMode::Require);
    config
}

/// The error `connect_raw` fails with against `script`. A script that panics fails the test
/// (with the client's result alongside).
async fn connect_error<T, F, Fut>(config: Config, tls: T, script: F) -> Error
where
    T: TlsConnect<DuplexStream>,
    F: FnOnce(Server) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (client_end, server_end) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(script(Server { s: server_end }));
    let result = config.connect_raw(client_end, tls).await.map(|_| ());
    if let Err(panic) = server.await {
        panic!("the scripted server failed ({panic:?}); the client got {result:?}");
    }
    result.expect_err("the connection should fail")
}

fn assert_flags(err: &Error, sasl_verified: bool, is_tls: bool, channel_binding: bool) {
    assert_eq!(err.sasl_verified(), sasl_verified, "sasl_verified: {err:?}");
    assert_eq!(err.is_tls(), is_tls, "is_tls: {err:?}");
    assert_eq!(
        err.is_channel_binding_refused(),
        channel_binding,
        "is_channel_binding_refused: {err:?}"
    );
}

// ---- sasl_verified ----------------------------------------------------------------------

// U9: Postgres checks `clientcert=verify-full` after the password, and a CN mismatch reaches
// the client as 28P01 after a SASL final message that verified.
#[tokio::test]
async fn a_28p01_after_a_verified_scram_final_is_flagged() {
    let err = connect_error(plain_config(), NoTls, |mut s| async move {
        s.startup(None).await;
        s.verified_scram().await;
        s.error("28P01", "password authentication failed for user \"u\"")
            .await;
    })
    .await;
    assert_eq!(err.code(), Some(&SqlState::INVALID_PASSWORD), "{err:?}");
    assert_flags(&err, true, false, false);
}

#[tokio::test]
async fn a_28p01_after_a_verified_scram_plus_final_is_flagged() {
    let err = connect_error(tls_config(), FakeTls::ok(), |mut s| async move {
        s.startup(Some(b'S')).await;
        let (mechanism, binding, signature) = s.scram_until_final(&[SCRAM_PLUS, SCRAM]).await;
        assert_eq!(mechanism, SCRAM_PLUS);
        let mut expected = b"p=tls-server-end-point,,".to_vec();
        expected.extend_from_slice(END_POINT);
        assert_eq!(binding, expected, "the channel-binding data");
        s.scram_final(&signature).await;
        s.error("28P01", "password authentication failed for user \"u\"")
            .await;
    })
    .await;
    assert_eq!(err.code(), Some(&SqlState::INVALID_PASSWORD), "{err:?}");
    assert_flags(&err, true, false, false);
}

// A wrong password: the server refuses the client's proof and never sends a final message.
#[tokio::test]
async fn a_refused_scram_proof_is_not_verified() {
    let err = connect_error(plain_config(), NoTls, |mut s| async move {
        s.startup(None).await;
        s.scram_until_final(&[SCRAM]).await;
        s.error("28P01", "password authentication failed for user \"u\"")
            .await;
    })
    .await;
    assert_eq!(err.code(), Some(&SqlState::INVALID_PASSWORD), "{err:?}");
    assert_flags(&err, false, false, false);
}

// A server that doesn't know the password can't sign: the client refuses it, unverified.
#[tokio::test]
async fn a_bad_server_signature_is_not_verified() {
    for config in [plain_config(), tls_config()] {
        let tls = config.get_ssl_mode() == SslMode::Require;
        let err = connect_error(config, FakeTls::ok(), move |mut s| async move {
            s.startup(tls.then_some(b'S')).await;
            let offer: &[&str] = if tls { &[SCRAM_PLUS] } else { &[SCRAM] };
            s.scram_until_final(offer).await;
            s.scram_final(&[0; 32]).await;
            s.drain().await;
        })
        .await;
        assert!(
            err.as_db_error().is_none(),
            "the client's own refusal: {err:?}"
        );
        assert_flags(&err, false, false, false);
    }
}

#[tokio::test]
async fn an_md5_refusal_is_not_verified() {
    let err = connect_error(plain_config(), NoTls, |mut s| async move {
        s.startup(None).await;
        s.auth(5, b"salt").await;
        let (tag, _) = s.read_msg().await;
        assert_eq!(tag, b'p', "the md5 password");
        s.error("28P01", "password authentication failed for user \"u\"")
            .await;
    })
    .await;
    assert_eq!(err.code(), Some(&SqlState::INVALID_PASSWORD), "{err:?}");
    assert_flags(&err, false, false, false);
}

#[tokio::test]
async fn an_error_before_authentication_is_not_verified() {
    let err = connect_error(plain_config(), NoTls, |mut s| async move {
        s.startup(None).await;
        s.error(
            "28000",
            "pg_hba.conf rejects connection for host \"10.0.0.1\", user \"u\", database \"app\", SSL encryption",
        )
        .await;
    })
    .await;
    assert_eq!(
        err.code(),
        Some(&SqlState::INVALID_AUTHORIZATION_SPECIFICATION),
        "{err:?}"
    );
    assert_flags(&err, false, false, false);
}

// The flag holds for every error the startup returns after the verified final message: one
// sent after AuthenticationOk, and a connection closed after the final message.
#[tokio::test]
async fn errors_later_in_the_startup_keep_the_flag() {
    let err = connect_error(plain_config(), NoTls, |mut s| async move {
        s.startup(None).await;
        s.verified_scram().await;
        s.auth(0, b"").await;
        s.error("53300", "sorry, too many clients already").await;
    })
    .await;
    assert_eq!(err.code(), Some(&SqlState::TOO_MANY_CONNECTIONS), "{err:?}");
    assert_flags(&err, true, false, false);

    let err = connect_error(plain_config(), NoTls, |mut s| async move {
        s.startup(None).await;
        s.verified_scram().await;
    })
    .await;
    assert!(err.is_closed(), "{err:?}");
    assert_flags(&err, true, false, false);
}

// Only `connect`'s errors carry it: a query's error on the open connection doesn't.
#[tokio::test]
async fn a_query_error_on_an_open_connection_is_not_flagged() {
    let (client_end, server_end) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(async move {
        let mut s = Server { s: server_end };
        s.startup(None).await;
        s.verified_scram().await;
        s.auth(0, b"").await;
        s.ready().await;
        let (tag, _) = s.read_msg().await;
        assert_eq!(tag, b'Q');
        s.error("42P01", "relation \"nope\" does not exist").await;
        s.send(b'Z', b"I").await;
        s.drain().await;
    });
    let (client, connection) = plain_config()
        .connect_raw(client_end, NoTls)
        .await
        .expect("the scripted startup succeeds");
    let driver = tokio::spawn(connection);
    let err = client.simple_query("SELECT * FROM nope").await.unwrap_err();
    assert_eq!(err.code(), Some(&SqlState::UNDEFINED_TABLE), "{err:?}");
    assert_flags(&err, false, false, false);
    drop(client);
    let _ = driver.await;
    server.await.unwrap();
}

#[tokio::test]
async fn a_timeout_is_none_of_these() {
    assert_flags(&Error::__private_api_timeout(), false, false, false);
}

// ---- is_tls -----------------------------------------------------------------------------

#[tokio::test]
async fn a_failed_handshake_is_tls_and_not_verified() {
    let tls = FakeTls {
        fail: Some(|| io::Error::new(io::ErrorKind::InvalidData, "invalid peer certificate")),
        end_point: None,
    };
    let err = connect_error(tls_config(), tls, |mut s| async move {
        s.ssl_request(b'S').await;
        s.drain().await;
    })
    .await;
    assert_flags(&err, false, true, false);
}

#[tokio::test]
async fn a_server_that_refuses_tls_is_tls() {
    let err = connect_error(tls_config(), FakeTls::ok(), |mut s| async move {
        s.startup(Some(b'N')).await;
    })
    .await;
    assert_flags(&err, false, true, false);
}

// ---- is_channel_binding_refused ---------------------------------------------------------

#[tokio::test]
async fn authenticating_without_channel_binding_is_refused_when_required() {
    // trust (AuthenticationOk at once), md5, and SCRAM without -PLUS over TLS.
    for case in ["ok", "md5", "scram"] {
        let err = connect_error(tls_config(), FakeTls::ok(), move |mut s| async move {
            s.startup(Some(b'S')).await;
            match case {
                "ok" => s.auth(0, b"").await,
                "md5" => s.auth(5, b"salt").await,
                _ => s.auth(10, b"SCRAM-SHA-256\0\0").await,
            }
            s.drain().await;
        })
        .await;
        assert!(err.as_db_error().is_none(), "{case}: {err:?}");
        assert_flags(&err, false, false, true);
    }
}

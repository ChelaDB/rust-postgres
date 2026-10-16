//! Edges of CopyBoth beyond `replication.rs`, against a scripted server: what the client sends
//! when the duplex is dropped, when a refusal arrives in pieces, when the query starts no copy,
//! and after the copy has ended.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::time::timeout;
use tokio_postgres::config::{ReplicationMode, SslMode};
use tokio_postgres::replication::ReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, Config, NoTls};

const START: &str = "START_REPLICATION SLOT \"rt_test\" LOGICAL 0/0";
const PATIENCE: Duration = Duration::from_secs(5);
/// Long enough for a connection task to write a stray message, if it were going to.
const SETTLE: Duration = Duration::from_millis(100);

struct Server {
    s: DuplexStream,
}

impl Server {
    async fn accept(&mut self) {
        let len = self.s.read_i32().await.unwrap();
        let mut body = vec![0; usize::try_from(len).unwrap() - 4];
        self.s.read_exact(&mut body).await.unwrap();
        self.send(b'R', &0i32.to_be_bytes()).await;
        self.ready().await;
    }

    async fn read_msg(&mut self) -> (u8, Vec<u8>) {
        let tag = self.s.read_u8().await.unwrap();
        let len = self.s.read_i32().await.unwrap();
        let mut body = vec![0; usize::try_from(len).unwrap() - 4];
        self.s.read_exact(&mut body).await.unwrap();
        (tag, body)
    }

    async fn query(&mut self) -> String {
        let (tag, body) = self.read_msg().await;
        assert_eq!(tag, b'Q', "a simple Query (got tag {:?})", tag as char);
        String::from_utf8(body[..body.len() - 1].to_vec()).unwrap()
    }

    async fn send(&mut self, tag: u8, body: &[u8]) {
        let mut out = vec![tag];
        out.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
        out.extend_from_slice(body);
        self.s.write_all(&out).await.unwrap();
    }

    async fn ready(&mut self) {
        self.send(b'Z', b"I").await;
    }

    async fn command_complete(&mut self, tag: &str) {
        let mut body = tag.as_bytes().to_vec();
        body.push(0);
        self.send(b'C', &body).await;
    }

    async fn error(&mut self, code: &str) {
        let mut body = Vec::new();
        for (field, value) in [(b'S', "ERROR"), (b'C', code), (b'M', "refused")] {
            body.push(field);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        self.send(b'E', &body).await;
    }

    async fn start_copy_both(&mut self) {
        assert_eq!(self.query().await, START);
        self.send(b'W', &[0, 0, 0]).await;
    }

    async fn xlog_data(&mut self, data: &[u8]) {
        let mut body = vec![b'w'];
        body.extend_from_slice(&[0; 24]);
        body.extend_from_slice(data);
        self.send(b'd', &body).await;
    }

    async fn answer_query(&mut self, expected: &str) {
        assert_eq!(self.query().await, expected);
        self.command_complete(expected).await;
        self.ready().await;
    }
}

fn replication_config() -> Config {
    let mut config = Config::new();
    config
        .user("chelabase_realtime")
        .dbname("app")
        .ssl_mode(SslMode::Disable)
        .replication_mode(ReplicationMode::Logical);
    config
}

async fn connected<F, Fut>(script: F) -> (Client, tokio::task::JoinHandle<()>)
where
    F: FnOnce(Server) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (client_end, server_end) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(script(Server { s: server_end }));
    let (client, connection) = replication_config()
        .connect_raw(client_end, NoTls)
        .await
        .expect("the scripted server accepts the connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, server)
}

async fn finish_script(server: tokio::task::JoinHandle<()>) {
    match timeout(PATIENCE, server).await {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => panic!("the scripted server failed: {panic:?}"),
        Err(_) => panic!("the scripted server did not finish"),
    }
}

async fn identify(client: &Client) {
    timeout(PATIENCE, client.simple_query("IDENTIFY_SYSTEM"))
        .await
        .expect("answered in time")
        .expect("the connection runs the next command");
}

// Dropping a running duplex ends the copy with exactly one CopyDone (never CopyFail or Sync);
// the connection discards the rest of the server's answer and runs the next command.
#[tokio::test]
async fn dropping_a_running_duplex_sends_one_copy_done_and_the_connection_goes_on() {
    let (client, server) = connected(|mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.xlog_data(b"unread").await;
        let (tag, body) = s.read_msg().await;
        assert_eq!(tag, b'c', "CopyDone (got tag {:?})", tag as char);
        assert!(body.is_empty());
        s.xlog_data(b"in flight").await;
        s.send(b'c', &[]).await;
        s.command_complete("START_REPLICATION").await;
        s.ready().await;
        s.answer_query("IDENTIFY_SYSTEM").await;
    })
    .await;
    let duplex = client
        .copy_both_simple::<Bytes>(START)
        .await
        .expect("the copy starts");
    drop(duplex);
    identify(&client).await;
    drop(client);
    finish_script(server).await;
}

// The refusal's ErrorResponse and ReadyForQuery arrive in separate reads: the client returns
// the error after the first and sends nothing for the copy that never started.
#[tokio::test]
async fn a_refusal_in_two_pieces_leaves_no_stray_message() {
    let (refused_tx, refused_rx) = tokio::sync::oneshot::channel::<()>();
    let (client, server) = connected(|mut s| async move {
        s.accept().await;
        assert_eq!(s.query().await, START);
        s.error("42501").await;
        s.s.flush().await.unwrap();
        refused_rx.await.unwrap();
        tokio::time::sleep(SETTLE).await;
        s.ready().await;
        s.answer_query("IDENTIFY_SYSTEM").await;
    })
    .await;
    let err = match client.copy_both_simple::<Bytes>(START).await {
        Ok(_) => panic!("refused"),
        Err(err) => err,
    };
    assert_eq!(err.as_db_error().map(|e| e.code().code()), Some("42501"));
    refused_tx.send(()).unwrap();
    identify(&client).await;
    drop(client);
    finish_script(server).await;
}

// A query that answers without a CopyBothResponse is an error, and the connection runs the
// next command.
#[tokio::test]
async fn a_query_that_starts_no_copy_is_an_error_and_the_connection_goes_on() {
    let (client, server) = connected(|mut s| async move {
        s.accept().await;
        s.answer_query(START).await;
        s.answer_query("IDENTIFY_SYSTEM").await;
    })
    .await;
    assert!(client.copy_both_simple::<Bytes>(START).await.is_err());
    identify(&client).await;
    drop(client);
    finish_script(server).await;
}

// After the server's error ended the copy, sending is an error, not a hang or a stray message,
// and the connection runs the next command.
#[tokio::test]
async fn sending_after_the_servers_error_is_an_error() {
    let (client, server) = connected(|mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.error("XX000").await;
        s.ready().await;
        tokio::time::sleep(SETTLE).await;
        s.answer_query("IDENTIFY_SYSTEM").await;
    })
    .await;
    let mut stream = Box::pin(ReplicationStream::new(
        client
            .copy_both_simple::<Bytes>(START)
            .await
            .expect("the copy starts"),
    ));
    assert!(matches!(
        timeout(PATIENCE, stream.next()).await.unwrap(),
        Some(Err(_))
    ));
    let sent = timeout(
        PATIENCE,
        stream.as_mut().standby_status_update(
            PgLsn::from(1),
            PgLsn::from(1),
            PgLsn::from(1),
            std::time::SystemTime::now(),
            0,
        ),
    )
    .await
    .expect("no hang");
    assert!(sent.is_err(), "the copy is over");
    assert!(
        timeout(PATIENCE, stream.as_mut().finish())
            .await
            .unwrap()
            .is_err(),
        "finishing an ended copy is an error"
    );
    drop(stream);
    identify(&client).await;
    drop(client);
    finish_script(server).await;
}

// After `finish`, the sink refuses items, the stream stays ended, and `finish` again is a no-op.
#[tokio::test]
async fn after_finish_the_duplex_is_closed() {
    let (client, server) = connected(|mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        let (tag, _) = s.read_msg().await;
        assert_eq!(tag, b'c');
        s.send(b'c', &[]).await;
        s.command_complete("START_REPLICATION").await;
        s.ready().await;
        tokio::time::sleep(SETTLE).await;
        s.answer_query("IDENTIFY_SYSTEM").await;
    })
    .await;
    let mut duplex = Box::pin(
        client
            .copy_both_simple::<Bytes>(START)
            .await
            .expect("the copy starts"),
    );
    timeout(PATIENCE, duplex.as_mut().finish())
        .await
        .unwrap()
        .expect("finished");
    assert!(duplex.send(Bytes::from_static(b"late")).await.is_err());
    assert!(timeout(PATIENCE, duplex.next()).await.unwrap().is_none());
    assert_eq!(
        timeout(PATIENCE, duplex.as_mut().finish())
            .await
            .unwrap()
            .unwrap(),
        0
    );
    drop(duplex);
    identify(&client).await;
    drop(client);
    finish_script(server).await;
}

#[test]
fn connection_strings_set_or_refuse_replication() {
    let config: Config = "postgresql://u@h/app?replication=database".parse().unwrap();
    assert_eq!(
        config.get_replication_mode(),
        Some(ReplicationMode::Logical)
    );
    for off in ["off", "false", "no", "0"] {
        let config: Config = format!("user=u replication=database replication={off}")
            .parse()
            .unwrap();
        assert_eq!(config.get_replication_mode(), None, "{off}");
    }
    // Physical replication is not supported: refused rather than taken for logical.
    assert!("user=u replication=true".parse::<Config>().is_err());
    assert_eq!(Config::new().get_replication_mode(), None);
}

//! Logical replication mode and CopyBoth against a scripted server on an in-memory stream:
//! the startup packet, `copy_both_simple`, `ReplicationStream` (XLogData, keepalives, standby
//! status updates), errors and the server or the client ending the copy.
//!
//! The startup rule these tests pin is the walsender's for `chelabase_realtime`: its startup
//! packet may set only `client_encoding` and `application_name`, besides `user`, `database`
//! and `replication`; any other parameter (`options` included) is refused.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use postgres_protocol::message::backend::ReplicationMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::time::timeout;
use tokio_postgres::config::{ReplicationMode, SslMode};
use tokio_postgres::replication::ReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, Config, CopyBothDuplex, Error, NoTls, SimpleQueryMessage};

const START: &str = "START_REPLICATION SLOT \"rt_test\" LOGICAL 0/0";
/// How long anything the tests wait for may take before the test fails instead of hanging.
const PATIENCE: Duration = Duration::from_secs(5);

/// 2000-01-01 00:00:00 UTC.
fn pg_epoch() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(946_684_800)
}

// ---- The scripted server ---------------------------------------------------------------

struct Server {
    s: DuplexStream,
}

impl Server {
    /// Reads the startup message and returns its parameters in the order sent.
    async fn startup(&mut self) -> Vec<(String, String)> {
        let len = self.s.read_i32().await.unwrap();
        let mut body = vec![0; usize::try_from(len).unwrap() - 4];
        self.s.read_exact(&mut body).await.unwrap();
        assert_eq!(&body[..4], &196_608i32.to_be_bytes(), "protocol 3.0");
        let mut fields = body[4..]
            .split(|b| *b == 0)
            .map(|f| String::from_utf8(f.to_vec()).unwrap());
        let mut params = Vec::new();
        loop {
            let key = fields.next().expect("a terminating NUL");
            if key.is_empty() {
                break;
            }
            let value = fields.next().expect("a value for every key");
            params.push((key, value));
        }
        params
    }

    /// Startup, trust authentication, ready for queries.
    async fn accept(&mut self) -> Vec<(String, String)> {
        let params = self.startup().await;
        self.send(b'R', &0i32.to_be_bytes()).await;
        let mut key = 42i32.to_be_bytes().to_vec();
        key.extend_from_slice(&7i32.to_be_bytes());
        self.send(b'K', &key).await;
        self.ready().await;
        params
    }

    async fn read_msg(&mut self) -> (u8, Vec<u8>) {
        let tag = self.s.read_u8().await.unwrap();
        let len = self.s.read_i32().await.unwrap();
        let mut body = vec![0; usize::try_from(len).unwrap() - 4];
        self.s.read_exact(&mut body).await.unwrap();
        (tag, body)
    }

    /// Reads a simple Query message and returns its text.
    async fn query(&mut self) -> String {
        let (tag, body) = self.read_msg().await;
        assert_eq!(tag, b'Q', "a simple Query (got tag {:?})", tag as char);
        assert_eq!(body.last(), Some(&0), "NUL-terminated");
        String::from_utf8(body[..body.len() - 1].to_vec()).unwrap()
    }

    /// Reads one CopyData message from the client and returns its payload.
    async fn copy_data(&mut self) -> Vec<u8> {
        let (tag, body) = self.read_msg().await;
        assert_eq!(tag, b'd', "CopyData (got tag {:?})", tag as char);
        body
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

    async fn error(&mut self, severity: &str, code: &str, message: &str) {
        let mut body = Vec::new();
        for (field, value) in [
            (b'S', severity),
            (b'V', severity),
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

    /// Answers the client's query with a CopyBothResponse, as a walsender answers
    /// `START_REPLICATION`.
    async fn start_copy_both(&mut self) {
        assert_eq!(self.query().await, START);
        self.send(b'W', &[0, 0, 0]).await;
    }

    async fn xlog_data(&mut self, wal_start: u64, wal_end: u64, ts: i64, data: &[u8]) {
        let mut body = vec![b'w'];
        body.extend_from_slice(&wal_start.to_be_bytes());
        body.extend_from_slice(&wal_end.to_be_bytes());
        body.extend_from_slice(&ts.to_be_bytes());
        body.extend_from_slice(data);
        self.send(b'd', &body).await;
    }

    async fn keepalive(&mut self, wal_end: u64, ts: i64, reply: u8) {
        let mut body = vec![b'k'];
        body.extend_from_slice(&wal_end.to_be_bytes());
        body.extend_from_slice(&ts.to_be_bytes());
        body.push(reply);
        self.send(b'd', &body).await;
    }

    /// Answers a simple query with an empty result, as a walsender answers a command with no
    /// rows, and checks that it is the query the client sent next (no stray message before).
    async fn answer_query(&mut self, expected: &str) {
        assert_eq!(self.query().await, expected);
        self.command_complete(expected).await;
        self.ready().await;
    }

    /// Reads one CopyData message and checks its payload (without printing kilobytes).
    async fn expect_copy_data(&mut self, expected: &[u8]) {
        let got = self.copy_data().await;
        assert!(
            got == expected,
            "CopyData of {} bytes starting {:?}, expected {} bytes starting {:?}",
            got.len(),
            &got[..got.len().min(16)],
            expected.len(),
            &expected[..expected.len().min(16)]
        );
    }
}

fn replication_config() -> Config {
    let mut config = Config::new();
    config
        .user("chelabase_realtime")
        .dbname("app")
        .application_name("chelabase_realtime")
        .ssl_mode(SslMode::Disable)
        .replication_mode(ReplicationMode::Logical);
    config
}

/// The startup parameters `config` sends (the server then refuses the connection).
async fn startup_params(config: Config) -> Vec<(String, String)> {
    let (client_end, server_end) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(async move {
        let mut s = Server { s: server_end };
        let params = s.startup().await;
        s.error("FATAL", "28000", "the test ends here").await;
        params
    });
    let result = config.connect_raw(client_end, NoTls).await;
    assert!(
        result.is_err(),
        "the scripted server refuses the connection"
    );
    server
        .await
        .expect("the scripted server read a startup packet")
}

fn as_map(params: &[(String, String)]) -> BTreeMap<&str, &str> {
    let map: BTreeMap<_, _> = params
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    assert_eq!(map.len(), params.len(), "no parameter twice: {params:?}");
    map
}

/// A connected client and the running script. The connection task runs in the background.
async fn connected<F, Fut>(config: Config, script: F) -> (Client, tokio::task::JoinHandle<()>)
where
    F: FnOnce(Server) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    connected_with_buffer(config, 64 * 1024, script).await
}

async fn connected_with_buffer<F, Fut>(
    config: Config,
    buffer: usize,
    script: F,
) -> (Client, tokio::task::JoinHandle<()>)
where
    F: FnOnce(Server) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (client_end, server_end) = tokio::io::duplex(buffer);
    let server = tokio::spawn(script(Server { s: server_end }));
    let (client, connection) = config
        .connect_raw(client_end, NoTls)
        .await
        .expect("the scripted server accepts the connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, server)
}

/// Waits for the script and fails the test with its panic, if it panicked.
async fn finish_script(server: tokio::task::JoinHandle<()>) {
    match timeout(PATIENCE, server).await {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => panic!("the scripted server failed: {panic:?}"),
        Err(_) => panic!("the scripted server did not finish"),
    }
}

async fn start(client: &Client) -> Pin<Box<ReplicationStream>> {
    let duplex = client
        .copy_both_simple::<Bytes>(START)
        .await
        .expect("a CopyBothResponse starts the copy");
    Box::pin(ReplicationStream::new(duplex))
}

async fn start_raw(client: &Client) -> Pin<Box<CopyBothDuplex<Bytes>>> {
    let duplex = client
        .copy_both_simple::<Bytes>(START)
        .await
        .expect("a CopyBothResponse starts the copy");
    Box::pin(duplex)
}

async fn next<S: futures_util::Stream + Unpin>(stream: &mut S) -> Option<S::Item> {
    timeout(PATIENCE, stream.next())
        .await
        .expect("the stream yields or ends in time")
}

fn db_code(err: &Error) -> Option<&str> {
    err.as_db_error().map(|e| e.code().code())
}

/// The bytes of a standby status update.
fn status_update(write: u64, flush: u64, apply: u64, micros: i64, reply: u8) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_u8(b'r');
    buf.put_u64(write);
    buf.put_u64(flush);
    buf.put_u64(apply);
    buf.put_i64(micros);
    buf.put_u8(reply);
    buf.to_vec()
}

/// What `chelabase_keys` sends: JSON, plus bytes no text decoder would keep as they are.
const PAYLOAD: &[u8] = b"{\"v\":1,\"lsn\":\"0/16B3748\",\"xid\":742,\"changes\":[{\"rel\":16384,\"s\":\"public\",\"t\":\"t\",\"op\":\"i\",\"k\":[[\"a\\\"b\\\\c \xc3\xbc \xe2\x9c\x93 \xf0\x9f\x98\x80\"]]}]}\x00\xff\r\n";

// ---- The startup packet -----------------------------------------------------------------

#[tokio::test]
async fn logical_replication_mode_sends_exactly_the_walsender_parameters() {
    let params = startup_params(replication_config()).await;
    assert_eq!(
        as_map(&params),
        BTreeMap::from([
            ("application_name", "chelabase_realtime"),
            ("client_encoding", "UTF8"),
            ("database", "app"),
            ("replication", "database"),
            ("user", "chelabase_realtime"),
        ]),
        "{params:?}"
    );
}

#[tokio::test]
async fn options_are_sent_only_when_configured() {
    let mut config = replication_config();
    config.options("-c search_path=x");
    let params = startup_params(config).await;
    assert_eq!(
        as_map(&params),
        BTreeMap::from([
            ("application_name", "chelabase_realtime"),
            ("client_encoding", "UTF8"),
            ("database", "app"),
            ("options", "-c search_path=x"),
            ("replication", "database"),
            ("user", "chelabase_realtime"),
        ]),
        "{params:?}"
    );
}

// Guard: without replication mode nothing changes (passes before the change too).
#[tokio::test]
async fn no_replication_parameter_without_replication_mode() {
    let mut config = Config::new();
    config
        .user("u")
        .dbname("app")
        .application_name("app")
        .ssl_mode(SslMode::Disable);
    let params = startup_params(config).await;
    assert_eq!(
        as_map(&params),
        BTreeMap::from([
            ("application_name", "app"),
            ("client_encoding", "UTF8"),
            ("database", "app"),
            ("user", "u"),
        ]),
        "{params:?}"
    );
}

#[tokio::test]
async fn a_connection_string_sets_replication_mode() {
    let config: Config = "user=u dbname=app sslmode=disable replication=database"
        .parse()
        .expect("replication=database parses");
    let params = startup_params(config).await;
    assert_eq!(
        as_map(&params).get("replication"),
        Some(&"database"),
        "{params:?}"
    );

    let config: Config = "user=u dbname=app sslmode=disable replication=off"
        .parse()
        .expect("replication=off parses");
    let params = startup_params(config).await;
    assert_eq!(as_map(&params).get("replication"), None, "{params:?}");

    assert!(
        "user=u replication=bogus".parse::<Config>().is_err(),
        "an unknown replication value is refused"
    );
}

// ---- Streaming --------------------------------------------------------------------------

#[tokio::test]
async fn xlog_data_arrives_with_its_header_and_a_byte_identical_payload() {
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.xlog_data(0x10, 0x20, 1_234_567, PAYLOAD).await;
        s.xlog_data(0x20, 0x30, 1_234_568, b"").await;
    })
    .await;
    let mut stream = start(&client).await;

    match next(&mut stream).await {
        Some(Ok(ReplicationMessage::XLogData(body))) => {
            assert_eq!((body.wal_start(), body.wal_end()), (0x10, 0x20));
            assert_eq!(
                body.timestamp(),
                pg_epoch() + Duration::from_micros(1_234_567)
            );
            assert_eq!(&body.data()[..], PAYLOAD);
        }
        other => panic!("expected XLogData, got {other:?}"),
    }
    match next(&mut stream).await {
        Some(Ok(ReplicationMessage::XLogData(body))) => {
            assert_eq!((body.wal_start(), body.wal_end()), (0x20, 0x30));
            assert!(body.data().is_empty());
        }
        other => panic!("expected an empty XLogData, got {other:?}"),
    }
    drop(stream);
    drop(client);
    finish_script(server).await;
}

#[tokio::test]
async fn a_keepalive_asking_for_a_reply_is_answered_with_a_standby_status_update() {
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.keepalive(0x0000_0001_0000_0040, 5_000_000, 1).await;
        assert_eq!(
            s.copy_data().await,
            status_update(
                0x0000_0001_0000_0040,
                0x0000_0001_0000_0030,
                0x0000_0001_0000_0020,
                6_000_001,
                0
            ),
            "the standby status update, field by field"
        );
    })
    .await;
    let mut stream = start(&client).await;

    let wal_end = match next(&mut stream).await {
        Some(Ok(ReplicationMessage::PrimaryKeepAlive(body))) => {
            assert_eq!(body.reply(), 1, "the server asks for a reply");
            assert_eq!(body.timestamp(), pg_epoch() + Duration::from_secs(5));
            body.wal_end()
        }
        other => panic!("expected a keepalive, got {other:?}"),
    };
    assert_eq!(wal_end, 0x0000_0001_0000_0040);
    timeout(
        PATIENCE,
        stream.as_mut().standby_status_update(
            PgLsn::from(wal_end),
            PgLsn::from(0x0000_0001_0000_0030),
            PgLsn::from(0x0000_0001_0000_0020),
            pg_epoch() + Duration::from_micros(6_000_001),
            0,
        ),
    )
    .await
    .expect("the update is sent in time")
    .expect("the update is sent");
    drop(stream);
    drop(client);
    finish_script(server).await;
}

#[tokio::test]
async fn a_standby_status_update_before_the_pg_epoch_has_a_negative_timestamp() {
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        assert_eq!(
            s.copy_data().await,
            status_update(1, 1, 1, -946_684_800_000_000, 1)
        );
    })
    .await;
    let mut stream = start(&client).await;
    stream
        .as_mut()
        .standby_status_update(
            PgLsn::from(1),
            PgLsn::from(1),
            PgLsn::from(1),
            UNIX_EPOCH,
            1,
        )
        .await
        .expect("the update is sent");
    drop(stream);
    drop(client);
    finish_script(server).await;
}

// Every item sent through the sink is one CopyData message: a walsender reads one standby
// message per CopyData, so two updates merged into one message would lose the second.
#[tokio::test]
async fn each_sink_item_is_its_own_copy_data_message() {
    let big = vec![7u8; 10_000];
    let expected_big = big.clone();
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.expect_copy_data(b"a").await;
        s.expect_copy_data(b"bc").await;
        s.expect_copy_data(&expected_big).await;
        s.expect_copy_data(b"d").await;
    })
    .await;
    let mut duplex = start_raw(&client).await;
    duplex.feed(Bytes::from_static(b"a")).await.expect("feed a");
    duplex
        .feed(Bytes::from_static(b"bc"))
        .await
        .expect("feed bc");
    duplex.feed(Bytes::from(big)).await.expect("feed 10 kB");
    duplex.feed(Bytes::from_static(b"d")).await.expect("feed d");
    timeout(PATIENCE, duplex.flush())
        .await
        .expect("flushed in time")
        .expect("flushed");
    drop(duplex);
    drop(client);
    finish_script(server).await;
}

// A server that stops reading makes the sender wait instead of buffering without bound;
// once it reads again, every update that was sent arrives, in order and intact.
#[tokio::test]
async fn standby_status_updates_wait_for_a_server_that_does_not_read() {
    let (go_tx, go_rx) = tokio::sync::oneshot::channel::<u64>();
    let (client, server) = connected_with_buffer(replication_config(), 4096, |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        // Not reading until the client has been made to wait.
        let sent = go_rx.await.unwrap();
        for i in 1..=sent {
            assert_eq!(
                s.copy_data().await,
                status_update(i, i, i, 0, 0),
                "update {i}"
            );
        }
    })
    .await;
    let mut stream = start(&client).await;

    let mut sent = 0u64;
    let mut waited = false;
    while sent < 100_000 {
        let i = sent + 1;
        let lsn = PgLsn::from(i);
        let update = stream
            .as_mut()
            .standby_status_update(lsn, lsn, lsn, pg_epoch(), 0);
        match timeout(Duration::from_millis(300), update).await {
            Ok(result) => {
                result.expect("the update is sent");
                sent = i;
            }
            Err(_) => {
                waited = true;
                break;
            }
        }
    }
    assert!(
        waited,
        "{sent} updates buffered without waiting for the server"
    );
    // The update whose send was cut off may or may not have been queued; the server checks
    // only the ones that completed.
    go_tx.send(sent).unwrap();
    drop(stream);
    drop(client);
    finish_script(server).await;
}

// ---- Errors and the end of the copy -----------------------------------------------------

#[tokio::test]
async fn a_refused_start_replication_is_an_error_and_the_connection_stays_usable() {
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        assert_eq!(s.query().await, START);
        s.error(
            "ERROR",
            "42501",
            "permission denied for replication command",
        )
        .await;
        s.ready().await;
        s.answer_query("IDENTIFY_SYSTEM").await;
    })
    .await;
    let err = match client.copy_both_simple::<Bytes>(START).await {
        Ok(_) => panic!("a refused START_REPLICATION starts no copy"),
        Err(err) => err,
    };
    assert_eq!(db_code(&err), Some("42501"), "{err:?}");
    let rows = timeout(PATIENCE, client.simple_query("IDENTIFY_SYSTEM"))
        .await
        .expect("answered in time")
        .expect("the connection is still usable");
    assert!(matches!(
        rows.last(),
        Some(SimpleQueryMessage::CommandComplete(_))
    ));
    drop(client);
    finish_script(server).await;
}

#[tokio::test]
async fn an_error_mid_stream_is_returned_once_then_the_stream_ends() {
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.xlog_data(1, 2, 0, b"first").await;
        s.error("ERROR", "XX000", "boom").await;
        s.ready().await;
        s.answer_query("IDENTIFY_SYSTEM").await;
    })
    .await;
    let mut stream = start(&client).await;
    assert!(matches!(
        next(&mut stream).await,
        Some(Ok(ReplicationMessage::XLogData(_)))
    ));
    match next(&mut stream).await {
        Some(Err(err)) => assert_eq!(db_code(&err), Some("XX000"), "{err:?}"),
        other => panic!("expected the server's error, got {other:?}"),
    }
    assert!(
        next(&mut stream).await.is_none(),
        "the stream ends after the error"
    );
    drop(stream);
    let rows = timeout(PATIENCE, client.simple_query("IDENTIFY_SYSTEM"))
        .await
        .expect("answered in time")
        .expect("the connection is still usable after an ERROR");
    assert!(matches!(
        rows.last(),
        Some(SimpleQueryMessage::CommandComplete(_))
    ));
    drop(client);
    finish_script(server).await;
}

#[tokio::test]
async fn a_fatal_error_and_a_closed_socket_end_the_stream() {
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.xlog_data(1, 2, 0, b"first").await;
        s.error(
            "FATAL",
            "57P01",
            "terminating connection due to administrator command",
        )
        .await;
        // The server goes away.
    })
    .await;
    let mut stream = start(&client).await;
    assert!(matches!(
        next(&mut stream).await,
        Some(Ok(ReplicationMessage::XLogData(_)))
    ));
    match next(&mut stream).await {
        Some(Err(err)) => assert_eq!(db_code(&err), Some("57P01"), "{err:?}"),
        other => panic!("expected the server's FATAL, got {other:?}"),
    }
    assert!(next(&mut stream).await.is_none(), "the stream ends");
    finish_script(server).await;
}

#[tokio::test]
async fn a_lost_connection_mid_stream_is_an_error_then_the_stream_ends() {
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.xlog_data(1, 2, 0, b"first").await;
        // The socket closes without a word.
    })
    .await;
    let mut stream = start(&client).await;
    assert!(matches!(
        next(&mut stream).await,
        Some(Ok(ReplicationMessage::XLogData(_)))
    ));
    match next(&mut stream).await {
        Some(Err(_)) => {}
        other => panic!("expected an error for the lost connection, got {other:?}"),
    }
    assert!(next(&mut stream).await.is_none(), "the stream ends");
    drop(client);
    finish_script(server).await;
}

#[tokio::test]
async fn an_unknown_replication_message_is_a_parse_error() {
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.keepalive(9, 0, 0).await;
        s.send(b'd', b"x-not-a-replication-message").await;
    })
    .await;
    let mut stream = start(&client).await;
    assert!(
        matches!(
            next(&mut stream).await,
            Some(Ok(ReplicationMessage::PrimaryKeepAlive(_)))
        ),
        "the keepalive before it parses"
    );
    assert!(
        matches!(next(&mut stream).await, Some(Err(_))),
        "an unknown tag is an error, not a panic or a skipped message"
    );
    drop(stream);
    drop(client);
    finish_script(server).await;
}

// The server ends the copy (CopyDone): the stream ends, `finish` sends the client's CopyDone
// and reads the command's completion, and the connection runs the next command. The simple
// query protocol has no Sync after CopyDone: the next message the server reads is the Query.
#[tokio::test]
async fn the_server_ending_the_copy_ends_the_stream_and_finish_completes_it() {
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.xlog_data(1, 2, 0, b"last").await;
        s.send(b'c', &[]).await;
        let (tag, _) = s.read_msg().await;
        assert_eq!(
            tag, b'c',
            "the client's CopyDone (got tag {:?})",
            tag as char
        );
        s.command_complete("START_REPLICATION").await;
        s.ready().await;
        s.answer_query("IDENTIFY_SYSTEM").await;
        s.answer_query("IDENTIFY_SYSTEM").await;
    })
    .await;
    let mut duplex = start_raw(&client).await;
    let first = next(&mut duplex).await;
    assert!(
        matches!(&first, Some(Ok(data)) if data.first() == Some(&b'w')),
        "the XLogData payload: {first:?}"
    );
    assert!(
        next(&mut duplex).await.is_none(),
        "CopyDone ends the stream"
    );
    timeout(PATIENCE, duplex.as_mut().finish())
        .await
        .expect("finished in time")
        .expect("finish completes the copy");
    drop(duplex);
    for _ in 0..2 {
        timeout(PATIENCE, client.simple_query("IDENTIFY_SYSTEM"))
            .await
            .expect("answered in time")
            .expect("the connection runs the next command");
    }
    drop(client);
    finish_script(server).await;
}

// The client ends the copy while the server is still streaming: `finish` skips what the
// server sent before its CopyDone and completes; the connection runs the next command.
#[tokio::test]
async fn finishing_mid_stream_skips_pending_data_and_completes() {
    let (client, server) = connected(replication_config(), |mut s| async move {
        s.accept().await;
        s.start_copy_both().await;
        s.xlog_data(1, 2, 0, b"in flight").await;
        s.keepalive(2, 0, 0).await;
        let (tag, _) = s.read_msg().await;
        assert_eq!(
            tag, b'c',
            "the client's CopyDone (got tag {:?})",
            tag as char
        );
        s.xlog_data(2, 3, 0, b"after the client's CopyDone").await;
        s.send(b'c', &[]).await;
        s.command_complete("START_REPLICATION").await;
        s.ready().await;
        s.answer_query("IDENTIFY_SYSTEM").await;
    })
    .await;
    let mut duplex = start_raw(&client).await;
    timeout(PATIENCE, duplex.as_mut().finish())
        .await
        .expect("finished in time")
        .expect("finish completes the copy");
    drop(duplex);
    timeout(PATIENCE, client.simple_query("IDENTIFY_SYSTEM"))
        .await
        .expect("answered in time")
        .expect("the connection runs the next command");
    drop(client);
    finish_script(server).await;
}

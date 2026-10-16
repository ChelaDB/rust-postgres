//! Logical replication against a real server: the `postgres-replication` service of
//! `docker-compose.yml` (port 5434, `wal_level = logical`; the main test server on 5433 keeps
//! its own configuration). Run `docker compose up -d` first, as for the rest of the suite.
//!
//! Every test uses its own table and its own TEMPORARY slot (`test_decoding`), so they run in
//! parallel; a slot sees the whole database, so each one picks out its own transaction.

use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use futures_util::StreamExt;
use postgres_protocol::message::backend::ReplicationMessage;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_postgres::config::{ReplicationMode, SslMode};
use tokio_postgres::replication::ReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, SimpleQueryRow};

const ADDR: &str = "127.0.0.1:5434";
const PATIENCE: Duration = Duration::from_secs(15);

/// A JSON body, the kind of payload `chelabase_keys` sends: quotes, backslashes, multi-byte
/// UTF-8 (2, 3 and 4 bytes). No single quote, so test_decoding prints it unchanged.
const BODY: &str = r#"{"v":1,"k":[["a\"b\\c","ü ✓ 😀"]],"all":false}"#;

fn unique(prefix: &str) -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!(
        "{prefix}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn sql_config() -> Config {
    let mut config = Config::new();
    config
        .user("postgres")
        .password("postgres")
        .dbname("postgres")
        .ssl_mode(SslMode::Disable);
    config
}

fn replication_config(application_name: &str) -> Config {
    let mut config = sql_config();
    config
        .application_name(application_name)
        .replication_mode(ReplicationMode::Logical);
    config
}

async fn connect(config: &Config) -> Client {
    let socket = TcpStream::connect(ADDR)
        .await
        .expect("the postgres-replication service of docker-compose.yml listens on 5434");
    let (client, connection) = config.connect_raw(socket, NoTls).await.expect("connects");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn rows(messages: Vec<SimpleQueryMessage>) -> Vec<SimpleQueryRow> {
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

/// `CREATE_REPLICATION_SLOT … TEMPORARY LOGICAL test_decoding`; returns its result row.
async fn create_slot(repl: &Client, slot: &str) -> SimpleQueryRow {
    let created = repl
        .simple_query(&format!(
            "CREATE_REPLICATION_SLOT {slot} TEMPORARY LOGICAL test_decoding (SNAPSHOT 'nothing')"
        ))
        .await
        .expect("a logical replication connection creates a temporary slot");
    let mut created = rows(created);
    assert_eq!(created.len(), 1, "one result row");
    created.remove(0)
}

async fn start(repl: &Client, slot: &str) -> Pin<Box<ReplicationStream>> {
    let duplex = repl
        .copy_both_simple::<Bytes>(&format!(
            "START_REPLICATION SLOT {slot} LOGICAL 0/0 (\"include-xids\" '0', \"skip-empty-xacts\" '1')"
        ))
        .await
        .expect("START_REPLICATION starts a CopyBoth stream");
    Box::pin(ReplicationStream::new(duplex))
}

async fn create_table(sql: &Client) -> String {
    let table = unique("rt_items");
    sql.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, body text NOT NULL)"
    ))
    .await
    .unwrap();
    table
}

/// Reads until the transaction that touched `table` commits; returns its messages (BEGIN to
/// COMMIT, byte for byte) and the COMMIT message's `wal_start`.
async fn transaction_on(
    stream: &mut Pin<Box<ReplicationStream>>,
    table: &str,
) -> (Vec<Vec<u8>>, u64) {
    let marker = format!("table public.{table}:");
    let deadline = Instant::now() + PATIENCE;
    let mut current: Vec<Vec<u8>> = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let message = timeout(left, stream.next())
            .await
            .expect("the transaction arrives in time")
            .expect("the stream stays open")
            .expect("no replication error");
        let body = match message {
            ReplicationMessage::XLogData(body) => body,
            ReplicationMessage::PrimaryKeepAlive(_) => continue,
            other => panic!("unexpected replication message {other:?}"),
        };
        let data = body.data().to_vec();
        if data == b"BEGIN" {
            current.clear();
        }
        let commit = data == b"COMMIT";
        current.push(data);
        if commit {
            let ours = current.iter().any(|m| m.starts_with(marker.as_bytes()));
            if ours {
                return (current, body.wal_start());
            }
            current.clear();
        }
    }
}

#[tokio::test]
async fn identify_system_runs_on_a_logical_replication_connection() {
    let application_name = unique("chelabase_realtime");
    let repl = connect(&replication_config(&application_name)).await;
    let identified = repl
        .simple_query("IDENTIFY_SYSTEM")
        .await
        .expect("IDENTIFY_SYSTEM runs on a logical walsender");
    let identified = rows(identified);
    assert_eq!(identified.len(), 1);
    let row = &identified[0];
    assert_eq!(row.get("dbname"), Some("postgres"), "a database walsender");
    assert!(row.get("systemid").is_some_and(|s| !s.is_empty()));
    let xlogpos: PgLsn = row.get("xlogpos").unwrap().parse().expect("an LSN");
    assert!(u64::from(xlogpos) > 0);

    // The server sees it as a walsender with the configured application name.
    let sql = connect(&sql_config()).await;
    let walsenders = sql
        .query(
            "SELECT 1 FROM pg_stat_activity WHERE backend_type = 'walsender' AND application_name = $1",
            &[&application_name],
        )
        .await
        .unwrap();
    assert_eq!(walsenders.len(), 1);
}

// S8: a payload that is not pgoutput (test_decoding text carrying a JSON body) arrives as the
// server sent it, byte for byte.
#[tokio::test]
async fn a_temporary_test_decoding_slot_streams_one_insert_byte_identical() {
    let sql = connect(&sql_config()).await;
    let table = create_table(&sql).await;
    let repl = connect(&replication_config("chelabase_realtime")).await;
    let slot = unique("rt");
    let created = create_slot(&repl, &slot).await;
    assert_eq!(created.get("slot_name"), Some(slot.as_str()));
    assert_eq!(created.get("output_plugin"), Some("test_decoding"));
    let temporary = sql
        .query_one(
            "SELECT temporary FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap();
    assert!(temporary.get::<_, bool>(0), "the slot is temporary");

    sql.execute(&format!("INSERT INTO {table} VALUES (1, $1)"), &[&BODY])
        .await
        .unwrap();
    let mut stream = start(&repl, &slot).await;
    let (transaction, _) = transaction_on(&mut stream, &table).await;
    let insert = format!("table public.{table}: INSERT: id[integer]:1 body[text]:'{BODY}'");
    assert_eq!(
        transaction,
        vec![b"BEGIN".to_vec(), insert.into_bytes(), b"COMMIT".to_vec()],
        "as test_decoding sent it"
    );
}

#[tokio::test]
async fn a_standby_status_update_moves_the_slots_confirmed_flush() {
    let sql = connect(&sql_config()).await;
    let table = create_table(&sql).await;
    let repl = connect(&replication_config("chelabase_realtime")).await;
    let slot = unique("rt");
    create_slot(&repl, &slot).await;
    sql.execute(&format!("INSERT INTO {table} VALUES (1, 'x')"), &[])
        .await
        .unwrap();
    let mut stream = start(&repl, &slot).await;
    let (_, commit_lsn) = transaction_on(&mut stream, &table).await;
    let ack = PgLsn::from(commit_lsn);
    stream
        .as_mut()
        .standby_status_update(ack, ack, ack, SystemTime::now(), 0)
        .await
        .expect("the update is sent");

    let deadline = Instant::now() + PATIENCE;
    loop {
        let row = sql
            .query_one(
                "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .unwrap();
        let confirmed: PgLsn = row.get(0);
        if u64::from(confirmed) >= commit_lsn {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "confirmed_flush_lsn {confirmed} never reached {ack}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// A walsender with a short `wal_sender_timeout` asks for a reply (`reply = 1`); answering
// each one keeps the connection open past the timeout.
#[tokio::test]
async fn answering_keepalives_that_ask_for_a_reply_keeps_the_stream_open() {
    let repl = {
        let mut config = replication_config("chelabase_realtime");
        config.options("-c wal_sender_timeout=2s");
        connect(&config).await
    };
    let slot = unique("rt");
    create_slot(&repl, &slot).await;
    let mut stream = start(&repl, &slot).await;

    let started = Instant::now();
    let mut answered = 0;
    while started.elapsed() < Duration::from_secs(5) {
        let message = timeout(PATIENCE, stream.next())
            .await
            .expect("the server sends keepalives")
            .expect("the stream stays open while the keepalives are answered")
            .expect("no replication error");
        if let ReplicationMessage::PrimaryKeepAlive(body) = message {
            if body.reply() == 1 {
                let lsn = PgLsn::from(body.wal_end());
                stream
                    .as_mut()
                    .standby_status_update(lsn, lsn, lsn, SystemTime::now(), 0)
                    .await
                    .expect("the reply is sent");
                answered += 1;
            }
        }
    }
    assert!(
        answered >= 2,
        "{answered} keepalives asked for a reply in 5 s"
    );
}

// The client ends the copy: `finish` completes and the walsender runs the next commands
// (no stray message is left on the connection).
#[tokio::test]
async fn finishing_the_copy_leaves_the_walsender_usable() {
    let repl = connect(&replication_config("chelabase_realtime")).await;
    let slot = unique("rt");
    create_slot(&repl, &slot).await;
    let mut duplex = Box::pin(
        repl.copy_both_simple::<Bytes>(&format!("START_REPLICATION SLOT {slot} LOGICAL 0/0"))
            .await
            .expect("START_REPLICATION starts a CopyBoth stream"),
    );
    timeout(PATIENCE, duplex.as_mut().finish())
        .await
        .expect("finished in time")
        .expect("finish completes the copy");
    drop(duplex);
    for _ in 0..2 {
        let identified = timeout(PATIENCE, repl.simple_query("IDENTIFY_SYSTEM"))
            .await
            .expect("answered in time")
            .expect("the walsender runs the next command");
        assert_eq!(rows(identified).len(), 1);
    }
}

// The walsender is terminated mid-stream: its FATAL arrives as an error, then the stream ends.
#[tokio::test]
async fn a_terminated_walsender_ends_the_stream_with_its_error() {
    let sql = connect(&sql_config()).await;
    let repl = connect(&replication_config("chelabase_realtime")).await;
    let slot = unique("rt");
    create_slot(&repl, &slot).await;
    let mut stream = start(&repl, &slot).await;

    let deadline = Instant::now() + PATIENCE;
    loop {
        let terminated = sql
            .query_one(
                "SELECT coalesce(bool_or(pg_terminate_backend(active_pid)), false) \
                 FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL",
                &[&slot],
            )
            .await
            .unwrap();
        if terminated.get::<_, bool>(0) {
            break;
        }
        assert!(Instant::now() < deadline, "the slot never became active");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let err = loop {
        match timeout(PATIENCE, stream.next())
            .await
            .expect("the stream yields in time")
        {
            Some(Ok(ReplicationMessage::PrimaryKeepAlive(_))) => continue,
            Some(Ok(other)) => panic!("unexpected message {other:?}"),
            Some(Err(err)) => break err,
            None => panic!("the stream ended without the server's error"),
        }
    };
    assert_eq!(
        err.as_db_error().map(|e| e.code().code()),
        Some("57P01"),
        "admin shutdown: {err:?}"
    );
    assert!(
        timeout(PATIENCE, stream.next())
            .await
            .expect("the stream ends in time")
            .is_none(),
        "the stream ends after the error"
    );
}

//! Parsing the CopyBoth response and the replication messages a walsender sends inside
//! CopyData: XLogData (`w`) and primary keepalive (`k`).

use std::panic::AssertUnwindSafe;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::{Message, ReplicationMessage};

/// 2000-01-01 00:00:00 UTC, the epoch of the replication protocol's timestamps.
fn pg_epoch() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(946_684_800)
}

/// A backend message: tag, length (including itself), body.
fn backend(tag: u8, body: &[u8]) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_u8(tag);
    buf.put_i32(i32::try_from(body.len() + 4).unwrap());
    buf.put_slice(body);
    buf
}

fn copy_both_response(format: u8, columns: &[u16]) -> BytesMut {
    let mut body = vec![format];
    body.extend_from_slice(&u16::try_from(columns.len()).unwrap().to_be_bytes());
    for c in columns {
        body.extend_from_slice(&c.to_be_bytes());
    }
    backend(b'W', &body)
}

fn xlog_data(wal_start: u64, wal_end: u64, ts: i64, data: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(b'w');
    buf.put_u64(wal_start);
    buf.put_u64(wal_end);
    buf.put_i64(ts);
    buf.put_slice(data);
    buf.freeze()
}

fn keepalive(wal_end: u64, ts: i64, reply: u8) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(b'k');
    buf.put_u64(wal_end);
    buf.put_i64(ts);
    buf.put_u8(reply);
    buf.freeze()
}

/// What `chelabase_keys` sends: JSON, plus bytes no text decoder would keep as they are.
const PAYLOAD: &[u8] = b"{\"v\":1,\"lsn\":\"0/16B3748\",\"xid\":742,\"changes\":[{\"rel\":16384,\"s\":\"public\",\"t\":\"t\",\"op\":\"i\",\"k\":[[\"a\\\"b\\\\c \xc3\xbc \xe2\x9c\x93 \xf0\x9f\x98\x80\"]]}]}\x00\xff\r\n";

// ---- CopyBothResponse ('W') -------------------------------------------------------------

#[test]
fn copy_both_response_parses_with_no_columns() {
    let mut buf = copy_both_response(0, &[]);
    let message = Message::parse(&mut buf).expect("a CopyBothResponse parses");
    let body = match message {
        Some(Message::CopyBothResponse(body)) => body,
        Some(_) => panic!("parsed as another message"),
        None => panic!("a whole message reported as incomplete"),
    };
    assert_eq!(body.format(), 0);
    assert_eq!(body.column_formats().count().unwrap(), 0);
    assert!(buf.is_empty(), "the message is consumed");
}

#[test]
fn copy_both_response_parses_column_formats() {
    let mut buf = copy_both_response(1, &[1, 0, 1]);
    let body = match Message::parse(&mut buf).expect("a CopyBothResponse parses") {
        Some(Message::CopyBothResponse(body)) => body,
        _ => panic!("not a CopyBothResponse"),
    };
    assert_eq!(body.format(), 1);
    assert_eq!(
        body.column_formats().collect::<Vec<u16>>().unwrap(),
        vec![1, 0, 1]
    );
}

/// The whole message parses as a CopyBothResponse (the control of the negative tests).
fn assert_parses_as_copy_both(mut buf: BytesMut) {
    match Message::parse(&mut buf).expect("the whole CopyBothResponse parses") {
        Some(Message::CopyBothResponse(_)) => {}
        _ => panic!("the whole message is not a CopyBothResponse"),
    }
}

#[test]
fn a_partial_copy_both_response_waits_for_more_bytes() {
    let whole = copy_both_response(0, &[]);
    assert_parses_as_copy_both(whole.clone());
    let mut buf = BytesMut::from(&whole[..whole.len() - 1]);
    assert!(
        Message::parse(&mut buf).expect("no error").is_none(),
        "incomplete until the last byte arrives"
    );
}

#[test]
fn a_truncated_copy_both_response_is_an_error() {
    assert_parses_as_copy_both(copy_both_response(0, &[]));
    // Length says 5: the format byte only, no column count.
    let mut buf = backend(b'W', &[0]);
    assert!(Message::parse(&mut buf).is_err());
}

// ---- XLogData ('w') ---------------------------------------------------------------------

#[test]
fn xlog_data_parses_its_header_and_keeps_the_payload_byte_identical() {
    let buf = xlog_data(
        0x0000_0001_6B37_4800,
        0x0000_0001_6B37_9000,
        1_234_567,
        PAYLOAD,
    );
    let message = ReplicationMessage::parse(&buf).expect("an XLogData parses");
    let body = match message {
        ReplicationMessage::XLogData(body) => body,
        other => panic!("not XLogData: {other:?}"),
    };
    assert_eq!(body.wal_start(), 0x0000_0001_6B37_4800);
    assert_eq!(body.wal_end(), 0x0000_0001_6B37_9000);
    assert_eq!(
        body.timestamp(),
        pg_epoch() + Duration::from_micros(1_234_567)
    );
    assert_eq!(&body.data()[..], PAYLOAD);
}

#[test]
fn xlog_data_with_an_empty_payload_parses() {
    let buf = xlog_data(16, 32, 0, b"");
    match ReplicationMessage::parse(&buf).expect("an empty XLogData parses") {
        ReplicationMessage::XLogData(body) => {
            assert_eq!((body.wal_start(), body.wal_end()), (16, 32));
            assert_eq!(body.timestamp(), pg_epoch());
            assert!(body.data().is_empty());
        }
        other => panic!("not XLogData: {other:?}"),
    }
}

#[test]
fn xlog_data_shorter_than_its_header_is_an_error() {
    let whole = xlog_data(1, 2, 3, b"");
    assert!(
        matches!(
            ReplicationMessage::parse(&whole),
            Ok(ReplicationMessage::XLogData(_))
        ),
        "the whole header parses"
    );
    for len in 1..whole.len() {
        let buf = whole.slice(..len);
        assert!(
            ReplicationMessage::parse(&buf).is_err(),
            "{len} bytes of a 25-byte header"
        );
    }
}

// ---- Primary keepalive ('k') ------------------------------------------------------------

#[test]
fn keepalive_parses_wal_end_timestamp_and_reply() {
    for reply in [0, 1] {
        let buf = keepalive(0x0000_0002_0000_0010, 86_400_000_000, reply);
        match ReplicationMessage::parse(&buf).expect("a keepalive parses") {
            ReplicationMessage::PrimaryKeepAlive(body) => {
                assert_eq!(body.wal_end(), 0x0000_0002_0000_0010);
                assert_eq!(body.timestamp(), pg_epoch() + Duration::from_secs(86_400));
                assert_eq!(body.reply(), reply);
            }
            other => panic!("not a keepalive: {other:?}"),
        }
    }
}

#[test]
fn keepalive_without_its_reply_byte_is_an_error() {
    let whole = keepalive(1, 2, 1);
    assert!(
        matches!(
            ReplicationMessage::parse(&whole),
            Ok(ReplicationMessage::PrimaryKeepAlive(_))
        ),
        "the whole keepalive parses"
    );
    for len in 1..whole.len() {
        let buf = whole.slice(..len);
        assert!(
            ReplicationMessage::parse(&buf).is_err(),
            "{len} bytes of an 18-byte keepalive"
        );
    }
}

// ---- Timestamps -------------------------------------------------------------------------

#[test]
fn timestamps_before_the_pg_epoch_parse() {
    // 1970-01-01, as a walsender with a clock before 2000 would send it.
    let ts = -946_684_800_000_000;
    match ReplicationMessage::parse(&keepalive(1, ts, 0)).expect("a negative timestamp parses") {
        ReplicationMessage::PrimaryKeepAlive(body) => assert_eq!(body.timestamp(), UNIX_EPOCH),
        other => panic!("not a keepalive: {other:?}"),
    }
    match ReplicationMessage::parse(&xlog_data(1, 1, -1, b"x")).expect("-1 µs parses") {
        ReplicationMessage::XLogData(body) => {
            assert_eq!(body.timestamp(), pg_epoch() - Duration::from_micros(1))
        }
        other => panic!("not XLogData: {other:?}"),
    }
}

#[test]
fn extreme_timestamps_never_panic() {
    assert!(
        ReplicationMessage::parse(&keepalive(1, 1, 0)).is_ok(),
        "an ordinary keepalive parses"
    );
    // A hostile or broken server must not crash the client: an error or some time is fine.
    for ts in [i64::MIN, i64::MIN + 1, i64::MAX] {
        let k = keepalive(1, ts, 0);
        let w = xlog_data(1, 1, ts, b"x");
        let parsed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = ReplicationMessage::parse(&k);
            let _ = ReplicationMessage::parse(&w);
        }));
        assert!(parsed.is_ok(), "timestamp {ts} panicked");
    }
}

// ---- Anything else ----------------------------------------------------------------------

#[test]
fn an_empty_or_unknown_replication_message_is_an_error() {
    assert!(
        ReplicationMessage::parse(&keepalive(1, 1, 0)).is_ok(),
        "an ordinary keepalive parses"
    );
    assert!(ReplicationMessage::parse(&Bytes::new()).is_err(), "empty");
    for tag in [b'r', b'h', b'x', 0] {
        let mut buf = BytesMut::new();
        buf.put_u8(tag);
        buf.put_slice(&[0; 40]);
        assert!(
            ReplicationMessage::parse(&buf.freeze()).is_err(),
            "tag {tag}"
        );
    }
    // And the error is a parse error, not a panic, for every one-byte message.
    for tag in 0..=u8::MAX {
        let buf = Bytes::copy_from_slice(&[tag]);
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            ReplicationMessage::parse(&buf).is_err()
        }));
        assert_eq!(result.ok(), Some(true), "tag {tag}");
    }
}

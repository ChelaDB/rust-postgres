//! Edges of the replication message parser beyond `replication.rs`.

use bytes::{BufMut, Bytes, BytesMut};
use postgres_protocol::message::backend::ReplicationMessage;

#[test]
fn a_keepalive_with_trailing_bytes_is_an_error() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'k');
    buf.put_u64(1);
    buf.put_i64(0);
    buf.put_u8(0);
    assert!(ReplicationMessage::parse(&buf.clone().freeze()).is_ok());
    buf.put_u8(0);
    assert!(ReplicationMessage::parse(&buf.freeze()).is_err());
}

#[test]
fn a_large_xlog_data_payload_is_kept_whole() {
    let payload: Vec<u8> = (0..1_048_576u32).map(|i| (i % 251) as u8).collect();
    let mut buf = BytesMut::new();
    buf.put_u8(b'w');
    buf.put_u64(u64::MAX);
    buf.put_u64(u64::MAX);
    buf.put_i64(0);
    buf.put_slice(&payload);
    match ReplicationMessage::parse(&buf.freeze()).unwrap() {
        ReplicationMessage::XLogData(body) => {
            assert_eq!((body.wal_start(), body.wal_end()), (u64::MAX, u64::MAX));
            assert!(body.data()[..] == payload[..]);
        }
        other => panic!("not XLogData: {other:?}"),
    }
}

#[test]
fn extreme_timestamps_are_errors_or_times_never_garbage() {
    // Whatever the platform can represent parses; anything else is an error.
    for ts in [i64::MIN, i64::MAX] {
        let mut buf = BytesMut::new();
        buf.put_u8(b'k');
        buf.put_u64(1);
        buf.put_i64(ts);
        buf.put_u8(0);
        if let Ok(ReplicationMessage::PrimaryKeepAlive(body)) =
            ReplicationMessage::parse(&Bytes::from(buf))
        {
            let epoch = std::time::UNIX_EPOCH + std::time::Duration::from_secs(946_684_800);
            if ts > 0 {
                assert!(body.timestamp() > epoch);
            } else {
                assert!(body.timestamp() < epoch);
            }
        }
    }
}

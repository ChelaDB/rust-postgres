//! The replication protocol over CopyBoth: the walsender's XLogData and keepalive messages, and
//! the client's standby status updates.
//!
//! The XLogData payload is handed over byte for byte; decoding it (a logical decoding plugin's
//! output) is up to the caller.

use crate::Error;
use crate::copy_both::CopyBothDuplex;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, Stream};
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const STANDBY_STATUS_UPDATE_TAG: u8 = b'r';

/// 2000-01-01 00:00:00 UTC, the epoch of the replication protocol's timestamps.
fn pg_epoch() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(946_684_800)
}

/// Microseconds from the PG epoch to `time`, negative before it, saturating at the `i64` range.
fn pg_timestamp(time: SystemTime) -> i64 {
    match time.duration_since(pg_epoch()) {
        Ok(after) => i64::try_from(after.as_micros()).unwrap_or(i64::MAX),
        Err(before) => i64::try_from(before.duration().as_micros())
            .map(|micros| -micros)
            .unwrap_or(i64::MIN),
    }
}

pin_project! {
    /// Parses the replication messages of a CopyBoth stream and sends standby status updates.
    ///
    /// The stream ends when the server ends the copy and after the copy's first error; a
    /// message it cannot parse is an error of its own, and the stream goes on after it. End the
    /// replication with `finish`.
    #[project(!Unpin)]
    pub struct ReplicationStream {
        #[pin]
        stream: CopyBothDuplex<Bytes>,
    }
}

impl ReplicationStream {
    /// Wraps the CopyBoth stream `START_REPLICATION` opened.
    pub fn new(stream: CopyBothDuplex<Bytes>) -> Self {
        Self { stream }
    }

    /// Sends a standby status update (`r`): the positions written, flushed and applied, the
    /// client's clock, and whether the server should reply at once (`reply = 1`).
    ///
    /// It waits while the connection's write buffer is full, so a server that stops reading
    /// holds the sender back instead of letting updates pile up.
    pub async fn standby_status_update(
        self: Pin<&mut Self>,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        ts: SystemTime,
        reply: u8,
    ) -> Result<(), Error> {
        let mut buf = BytesMut::with_capacity(34);
        buf.put_u8(STANDBY_STATUS_UPDATE_TAG);
        buf.put_u64(write_lsn.into());
        buf.put_u64(flush_lsn.into());
        buf.put_u64(apply_lsn.into());
        buf.put_i64(pg_timestamp(ts));
        buf.put_u8(reply);

        self.project().stream.send(buf.freeze()).await
    }

    /// Ends the replication: see [`CopyBothDuplex::finish`].
    pub async fn finish(self: Pin<&mut Self>) -> Result<u64, Error> {
        self.project().stream.finish().await
    }
}

impl Stream for ReplicationStream {
    type Item = Result<ReplicationMessage<Bytes>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.project().stream.poll_next(cx)) {
            Some(Ok(buf)) => {
                Poll::Ready(Some(ReplicationMessage::parse(&buf).map_err(Error::parse)))
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_timestamps_saturate_and_round_trip() {
        assert_eq!(pg_timestamp(pg_epoch()), 0);
        assert_eq!(pg_timestamp(UNIX_EPOCH), -946_684_800_000_000);
        assert_eq!(
            pg_timestamp(pg_epoch() + Duration::from_micros(6_000_001)),
            6_000_001
        );
        assert_eq!(pg_timestamp(pg_epoch() - Duration::from_micros(1)), -1);
        let far = pg_epoch() + Duration::from_secs(u64::from(u32::MAX) * 1_000_000);
        assert_eq!(pg_timestamp(far), i64::MAX);
        if let Some(early) =
            pg_epoch().checked_sub(Duration::from_secs(u64::from(u32::MAX) * 1_000_000))
        {
            assert_eq!(pg_timestamp(early), i64::MIN);
        }
    }
}

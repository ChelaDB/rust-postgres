//! The replication protocol over CopyBoth (t2 RED: declarations only; the coder implements
//! them).

use crate::Error;
use crate::copy_both::CopyBothDuplex;
use bytes::Bytes;
use futures_util::Stream;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

/// Parses the replication messages of a CopyBoth stream and sends standby status updates.
pub struct ReplicationStream {
    stream: CopyBothDuplex<Bytes>,
}

impl ReplicationStream {
    /// Wraps the CopyBoth stream `START_REPLICATION` opened.
    pub fn new(stream: CopyBothDuplex<Bytes>) -> Self {
        Self { stream }
    }

    /// Sends a standby status update.
    pub async fn standby_status_update(
        self: Pin<&mut Self>,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        ts: SystemTime,
        reply: u8,
    ) -> Result<(), Error> {
        let _ = (&self.stream, write_lsn, flush_lsn, apply_lsn, ts, reply);
        Err(Error::closed())
    }
}

impl Stream for ReplicationStream {
    type Item = Result<ReplicationMessage<Bytes>, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

//! CopyBoth (t2 RED: declarations only; the coder implements them).

use crate::Error;
use bytes::{Buf, Bytes};
use futures_util::{Sink, Stream};
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::task::{Context, Poll};

/// The two halves of a CopyBoth exchange: a `Stream` of the server's CopyData payloads and a
/// `Sink` of the client's.
pub struct CopyBothDuplex<T> {
    _p: PhantomPinned,
    _p2: PhantomData<T>,
}

impl<T> CopyBothDuplex<T>
where
    T: Buf + 'static + Send,
{
    /// Completes the copy.
    pub async fn finish(self: Pin<&mut Self>) -> Result<u64, Error> {
        Err(Error::closed())
    }
}

impl<T> Stream for CopyBothDuplex<T> {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl<T> Sink<T> for CopyBothDuplex<T>
where
    T: Buf + 'static + Send,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Err(Error::closed()))
    }

    fn start_send(self: Pin<&mut Self>, _item: T) -> Result<(), Error> {
        Err(Error::closed())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Err(Error::closed()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Err(Error::closed()))
    }
}

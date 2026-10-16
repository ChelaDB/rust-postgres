//! CopyBoth: the duplex copy a walsender starts for `START_REPLICATION`.
//!
//! The copy runs on the simple query protocol, so the client never sends `Sync`: it ends the
//! copy with `CopyDone`, and the server answers with its own `CopyDone` (if it has not sent one
//! already), `CommandComplete` and `ReadyForQuery`. A `CopyFail` would make a walsender
//! terminate the connection, so it is never sent either.

use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{Error, simple_query};
use bytes::{Buf, Bytes, BytesMut};
use futures_channel::mpsc;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_protocol::message::frontend::CopyData;
use std::future;
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, ready};

pub(crate) enum CopyBothMessage {
    Message(FrontendMessage),
    Done,
}

/// The connection's side of the client's half of the copy.
///
/// The connection drops it as soon as the server completes the command (`ReadyForQuery`), so
/// nothing is sent after the server has left the copy.
pub struct CopyBothReceiver {
    receiver: mpsc::Receiver<CopyBothMessage>,
    /// Set once the server has started the copy (CopyBothResponse); only then does dropping the
    /// duplex end the copy with a `CopyDone`.
    started: Arc<AtomicBool>,
    done: bool,
}

impl CopyBothReceiver {
    fn new(receiver: mpsc::Receiver<CopyBothMessage>, started: Arc<AtomicBool>) -> Self {
        CopyBothReceiver {
            receiver,
            started,
            done: false,
        }
    }
}

fn copy_done() -> FrontendMessage {
    let mut buf = BytesMut::new();
    frontend::copy_done(&mut buf);
    FrontendMessage::Raw(buf.freeze())
}

impl Stream for CopyBothReceiver {
    type Item = FrontendMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<FrontendMessage>> {
        if self.done {
            return Poll::Ready(None);
        }

        match ready!(self.receiver.poll_next_unpin(cx)) {
            Some(CopyBothMessage::Message(message)) => Poll::Ready(Some(message)),
            Some(CopyBothMessage::Done) => {
                self.done = true;
                Poll::Ready(Some(copy_done()))
            }
            None => {
                // The duplex was dropped without `finish`, or the copy never started (the query
                // was refused): end a started copy, send nothing otherwise.
                self.done = true;
                if self.started.load(Ordering::Acquire) {
                    Poll::Ready(Some(copy_done()))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}

enum SinkState {
    Active,
    Closing,
    Reading,
    Finished,
}

pin_project! {
    /// The two halves of a CopyBoth exchange: a `Stream` of the server's CopyData payloads and a
    /// `Sink` of the client's, each item one CopyData message.
    ///
    /// The stream ends when the server ends the copy (`CopyDone`) and after the first error. End
    /// the copy with `finish` (or `Sink::close`); dropping the duplex of a running copy also ends
    /// it with a `CopyDone`, and the connection discards the rest of the server's answer.
    #[project(!Unpin)]
    pub struct CopyBothDuplex<T> {
        #[pin]
        sender: mpsc::Sender<CopyBothMessage>,
        responses: Responses,
        state: SinkState,
        stream_done: bool,
        #[pin]
        _p: PhantomPinned,
        _p2: PhantomData<T>,
    }
}

impl<T> CopyBothDuplex<T>
where
    T: Buf + 'static + Send,
{
    fn new(sender: mpsc::Sender<CopyBothMessage>, responses: Responses) -> Self {
        CopyBothDuplex {
            sender,
            responses,
            state: SinkState::Active,
            stream_done: false,
            _p: PhantomPinned,
            _p2: PhantomData,
        }
    }

    /// A poll-based version of `finish`.
    pub fn poll_finish(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<u64, Error>> {
        loop {
            match self.state {
                SinkState::Active => {
                    let mut this = self.as_mut().project();
                    ready!(this.sender.as_mut().poll_ready(cx)).map_err(|_| Error::closed())?;
                    this.sender
                        .as_mut()
                        .start_send(CopyBothMessage::Done)
                        .map_err(|_| Error::closed())?;
                    *this.state = SinkState::Closing;
                }
                SinkState::Closing => {
                    let this = self.as_mut().project();
                    ready!(this.sender.poll_close(cx)).map_err(|_| Error::closed())?;
                    *this.state = SinkState::Reading;
                }
                SinkState::Reading => {
                    let this = self.as_mut().project();
                    // Whatever the server sent before it saw the client's CopyDone (data, or its
                    // own CopyDone) is skipped; the command's completion ends the copy.
                    let message = match ready!(this.responses.poll_next(cx)) {
                        Ok(message) => message,
                        Err(e) => {
                            *this.state = SinkState::Finished;
                            *this.stream_done = true;
                            return Poll::Ready(Err(e));
                        }
                    };
                    match message {
                        Message::CopyData(_) => {}
                        Message::CopyDone => *this.stream_done = true,
                        Message::CommandComplete(body) => {
                            *this.state = SinkState::Finished;
                            *this.stream_done = true;
                            let tag = body.tag().map_err(Error::parse)?;
                            let rows = tag.rsplit(' ').next().unwrap_or("").parse().unwrap_or(0);
                            return Poll::Ready(Ok(rows));
                        }
                        _ => {
                            *this.state = SinkState::Finished;
                            *this.stream_done = true;
                            return Poll::Ready(Err(Error::unexpected_message()));
                        }
                    }
                }
                SinkState::Finished => return Poll::Ready(Ok(0)),
            }
        }
    }

    /// Ends the copy: sends the client's `CopyDone` and waits for the command to complete,
    /// skipping whatever the server still sends before it. Returns the row count of the
    /// command's tag (0 for `START_REPLICATION`).
    ///
    /// The `Sink::close` method is equivalent to `finish`, except that it does not return the
    /// count.
    pub async fn finish(mut self: Pin<&mut Self>) -> Result<u64, Error> {
        future::poll_fn(|cx| self.as_mut().poll_finish(cx)).await
    }
}

impl<T> Stream for CopyBothDuplex<T> {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.stream_done {
            return Poll::Ready(None);
        }

        let message = match ready!(this.responses.poll_next(cx)) {
            Ok(message) => message,
            Err(e) => {
                *this.stream_done = true;
                return Poll::Ready(Some(Err(e)));
            }
        };
        match message {
            Message::CopyData(body) => Poll::Ready(Some(Ok(body.into_bytes()))),
            Message::CopyDone => {
                *this.stream_done = true;
                Poll::Ready(None)
            }
            _ => {
                *this.stream_done = true;
                Poll::Ready(Some(Err(Error::unexpected_message())))
            }
        }
    }
}

impl<T> Sink<T> for CopyBothDuplex<T>
where
    T: Buf + 'static + Send,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.project()
            .sender
            .poll_ready(cx)
            .map_err(|_| Error::closed())
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Error> {
        let this = self.project();
        if !matches!(this.state, SinkState::Active) {
            return Err(Error::closed());
        }
        // One CopyData per item: a walsender reads one standby message per CopyData.
        let data: Box<dyn Buf + Send> = Box::new(item);
        let data = CopyData::new(data).map_err(Error::encode)?;
        this.sender
            .start_send(CopyBothMessage::Message(FrontendMessage::CopyData(data)))
            .map_err(|_| Error::closed())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.project()
            .sender
            .poll_flush(cx)
            .map_err(|_| Error::closed())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.poll_finish(cx).map_ok(|_| ())
    }
}

pub async fn copy_both_simple<T>(
    client: &InnerClient,
    query: &str,
) -> Result<CopyBothDuplex<T>, Error>
where
    T: Buf + 'static + Send,
{
    debug!("executing copy both query {query}");

    let buf = simple_query::encode(client, query)?;

    let (mut sender, receiver) = mpsc::channel(1);
    let started = Arc::new(AtomicBool::new(false));
    let receiver = CopyBothReceiver::new(receiver, started.clone());
    let mut responses = client.send(RequestMessages::CopyBoth(receiver))?;

    sender
        .send(CopyBothMessage::Message(FrontendMessage::Raw(buf)))
        .await
        .map_err(|_| Error::closed())?;

    match responses.next().await? {
        Message::CopyBothResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }
    started.store(true, Ordering::Release);

    Ok(CopyBothDuplex::new(sender, responses))
}

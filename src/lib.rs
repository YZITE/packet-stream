#![forbid(unsafe_code)]

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_core::{ready, stream};
use futures_io::{AsyncRead, AsyncWrite};
use futures_sink::Sink;
use std::task::{Context, Poll};
use std::{fmt, pin::Pin};

#[cfg(feature = "tracing")]
use tracing::{debug, instrument};

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct PacketStream<T> {
        #[pin]
        stream: T,
        buf_in: BytesMut,
        buf_out: BytesMut,
        in_got_eof: bool,
        in_xpdlen: Option<usize>,
    }
}

impl<T> PacketStream<T> {
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            buf_in: BytesMut::new(),
            buf_out: BytesMut::new(),
            in_got_eof: false,
            in_xpdlen: None,
        }
    }
}

macro_rules! pollerfwd {
    ($x:expr) => {{
        match ready!($x) {
            Ok(x) => x,
            Err(e) => return ::std::task::Poll::Ready(Err(e)),
        }
    }};
}

impl<T> stream::Stream for PacketStream<T>
where
    T: AsyncRead + fmt::Debug,
{
    type Item = std::io::Result<Bytes>;

    #[cfg_attr(feature = "tracing", instrument)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            if this.buf_in.len() >= 2 && this.in_xpdlen.is_none() {
                let expect_len: usize = this.buf_in.get_u16().into();
                *this.in_xpdlen = Some(expect_len);
                this.buf_in.reserve(expect_len);
            }
            if let Some(expect_len) = *this.in_xpdlen {
                if this.buf_in.len() >= expect_len {
                    // we are done, if we reach this,
                    // the length spec was already removed from buf_xin
                    *this.in_xpdlen = None;
                    return Poll::Ready(Some(Ok(this.buf_in.split_to(expect_len).freeze())));
                }
            }
            if *this.in_got_eof {
                // don't poll again bc we reached EOF
                return Poll::Ready(None);
            }

            // we need more data; the `read` might yield,
            // and it should not leave any part of `this` in an invalid state
            let tmp = poll_buf_utils::poll_read(this.stream.as_mut(), this.buf_in, cx, usize::from(u16::MAX) + 2);
            if tmp.delta != 0 {
                #[cfg(feature = "tracing")]
                debug!("received {} bytes", tmp.delta);
            }
            if let Poll::Ready(Ok(_reached_limit)) = &tmp.ret {
                #[cfg(feature = "tracing")]
                debug!("received EOF (reached_limit = {:?})", _reached_limit);

                *this.in_got_eof = true;
            }
            if tmp.delta == 0 {
                // yield to the executor
                return tmp.ret.map(|y| y.err().map(Err));
            }
            // we don't yield to the executor, this might lead to spurious wake-ups,
            // but we don't really care...
        }
    }
}

impl<T> stream::FusedStream for PacketStream<T>
where
    T: AsyncRead + fmt::Debug,
{
    fn is_terminated(&self) -> bool {
        self.in_got_eof && self.buf_in.len() < self.in_xpdlen.unwrap_or(2)
    }
}

type SinkYield = Poll<Result<(), std::io::Error>>;

impl<B, T> Sink<B> for PacketStream<T>
where
    B: AsRef<[u8]>,
    T: AsyncWrite + fmt::Debug,
{
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> SinkYield {
        if self.buf_out.len() > u16::MAX.into() {
            pollerfwd!(Sink::<B>::poll_flush(self, cx));
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: B) -> Result<(), std::io::Error> {
        use std::convert::TryInto;
        let buf_out = self.project().buf_out;
        let item = item.as_ref();
        buf_out.put_u16(item.len().try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "length overflow")
        })?);
        buf_out.extend_from_slice(item);
        Ok(())
    }

    #[cfg_attr(feature = "tracing", instrument)]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> SinkYield {
        let mut this = self.project();
        let tmp = poll_buf_utils::poll_write(this.buf_out, this.stream.as_mut(), cx);

        #[cfg(feature = "tracing")]
        if tmp.delta != 0 {
            debug!("sent {} bytes", tmp.delta);
        }

        pollerfwd!(tmp.ret);
        this.stream.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> SinkYield {
        pollerfwd!(Sink::<B>::poll_flush(self.as_mut(), cx));
        self.project().stream.poll_close(cx)
    }
}

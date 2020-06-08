#![forbid(unsafe_code)]

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_core::{ready, stream::Stream};
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
        in_xpdlen: Option<usize>,
    }
}

impl<T> PacketStream<T> {
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            buf_in: BytesMut::new(),
            buf_out: BytesMut::new(),
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

impl<T> Stream for PacketStream<T>
where
    T: AsyncRead + fmt::Debug,
{
    type Item = std::io::Result<Bytes>;

    #[cfg_attr(feature = "tracing", instrument)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            if this.buf_in.len() >= 2 && this.in_xpdlen.is_none() {
                *this.in_xpdlen = Some(this.buf_in.get_u16().into());
            }
            if let Some(expect_len) = *this.in_xpdlen {
                if this.buf_in.len() >= expect_len {
                    // we are done, if we reach this,
                    // the length spec was already removed from buf_xin
                    *this.in_xpdlen = None;
                    return Poll::Ready(Some(Ok(this.buf_in.split_to(expect_len).freeze())));
                }
            }

            // we need more data
            // the `read` might yield, and it should not leave any part of
            // `this` in an invalid state
            // assumption: `read` only yields if it has not read (and dropped) anything yet.
            let mut rdbuf = [0u8; 8192];
            match ready!(this.stream.as_mut().poll_read(cx, &mut rdbuf)) {
                Err(e) => return Poll::Ready(Some(Err(e))),
                Ok(0) => {
                    #[cfg(feature = "tracing")]
                    debug!("received EOF");

                    return Poll::Ready(None);
                }
                Ok(len) => {
                    #[cfg(feature = "tracing")]
                    debug!("received {} bytes", len);

                    this.buf_in.extend_from_slice(&rdbuf[..len]);
                }
            }
        }
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
        let buf_out = this.buf_out;
        let _old_len = buf_out.remaining();
        let tmp = poll_buf_utils::poll_write(buf_out, this.stream.as_mut(), cx);

        #[cfg(feature = "tracing")]
        {
            let dlen = _old_len - buf_out.remaining();
            if dlen != 0 {
                debug!("sent {} bytes", dlen);
            }
        }

        pollerfwd!(tmp);
        this.stream.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> SinkYield {
        pollerfwd!(Sink::<B>::poll_flush(self.as_mut(), cx));
        self.project().stream.poll_close(cx)
    }
}

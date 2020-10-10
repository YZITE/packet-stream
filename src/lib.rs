#![forbid(unsafe_code)]

use futures_core::{ready, stream};
use futures_io::{AsyncRead, AsyncWrite};
use std::task::{Context, Poll};
use std::{fmt, pin::Pin};

#[cfg(feature = "tracing")]
use tracing::{debug, instrument};

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct PacketStream<T> {
        #[pin]
        stream: T,
        buf_in: Vec<u8>,
        buf_out: Vec<u8>,
        in_got_eof: bool,
        in_xpdlen: Option<usize>,
    }
}

impl<T> PacketStream<T> {
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            buf_in: Vec::new(),
            buf_out: Vec::new(),
            in_got_eof: false,
            in_xpdlen: None,
        }
    }
}

impl<T> stream::Stream for PacketStream<T>
where
    T: AsyncRead + fmt::Debug,
{
    type Item = std::io::Result<Vec<u8>>;

    #[cfg_attr(feature = "tracing", instrument)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            if this.buf_in.len() >= 2 && this.in_xpdlen.is_none() {
                let mut tmp = [0u8; 2usize];
                tmp.copy_from_slice(&this.buf_in[..2]);
                let _ = this.buf_in.drain(..2);
                let expect_len: usize = u16::from_be_bytes(tmp).into();
                *this.in_xpdlen = Some(expect_len);
                if expect_len > this.buf_in.capacity() {
                    this.buf_in.reserve(expect_len - this.buf_in.len());
                }
            }
            if let Some(expect_len) = *this.in_xpdlen {
                if this.buf_in.len() >= expect_len {
                    // we are done, if we reach this,
                    // the length spec was already removed from buf_xin
                    *this.in_xpdlen = None;
                    let new_buf_in = this.buf_in.split_off(expect_len);
                    return Poll::Ready(Some(Ok(std::mem::replace(this.buf_in, new_buf_in))));
                }
            }
            if *this.in_got_eof {
                // don't poll again bc we reached EOF
                return Poll::Ready(None);
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

                    *this.in_got_eof = true;
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

impl<T> stream::FusedStream for PacketStream<T>
where
    T: AsyncRead + fmt::Debug,
{
    #[inline]
    fn is_terminated(&self) -> bool {
        self.in_got_eof && self.buf_in.len() < self.in_xpdlen.unwrap_or(2)
    }
}

type SinkYield = Poll<Result<(), std::io::Error>>;

impl<T> PacketStream<T>
where
    T: AsyncWrite + fmt::Debug,
{
    fn enqueue_intern(self: Pin<&mut Self>, item: &[u8]) -> Result<(), std::io::Error> {
        use std::convert::TryInto;
        let buf_out = self.project().buf_out;
        buf_out.extend_from_slice(&u16::to_be_bytes(item.len().try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "length overflow")
        })?));
        buf_out.extend_from_slice(item);
        Ok(())
    }

    /// generic sending method
    #[inline]
    pub fn enqueue<B: AsRef<[u8]>>(self: Pin<&mut Self>, item: B) -> Result<(), std::io::Error> {
        self.enqueue_intern(item.as_ref())
    }

    #[cfg_attr(feature = "tracing", instrument)]
    pub fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> SinkYield {
        let this = self.project();
        let (buf_out, mut stream) = (this.buf_out, this.stream);
        let mut offset = 0;

        let ret = loop {
            match stream.as_mut().poll_write(cx, &buf_out[offset..]) {
                // if we managed to write something...
                Poll::Ready(Ok(n)) if n != 0 => offset += n,

                // assumption: if we get here, the call to poll_write failed and
                // didn't write anything
                ret => break ret,
            }
        };

        if offset != 0 {
            let _ = buf_out.drain(..offset);

            #[cfg(feature = "tracing")]
            debug!("sent {} bytes", offset);
        }

        ready!(ret?);
        stream.poll_flush(cx)
    }

    pub fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> SinkYield {
        ready!(self.as_mut().poll_flush(cx)?);
        self.project().stream.poll_close(cx)
    }
}

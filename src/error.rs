use std::{io, str::Utf8Error};

/// All possible errors returned by this library.
pub enum Error {
    /// An IO error happened on the underlying TCP stream
    IoError(io::Error),
    /// You attempted to unsubscribe from a channel that you were not subscribed to
    NotSubscribed,
    /// Zero bytes were read from the TCP socket: this is an IO error and is usually fatal.
    ZeroBytesRead,
    /// An error happened while decoding the data from Redis as UTF-8.
    Utf8Error(Utf8Error),
    /// The parser implementation returned an error.
    ParserError(crate::message::MessageParserError),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::IoError(e)
    }
}

impl From<Utf8Error> for Error {
    fn from(e: Utf8Error) -> Self {
        Self::Utf8Error(e)
    }
}

/// An wrapper around the standard [Result] type with [Error] aliased to this crate's error type.
///
/// [Result]: std::result::Result
/// [Error]: crate::error::Error
pub type Result<T> = std::result::Result<T, Error>;

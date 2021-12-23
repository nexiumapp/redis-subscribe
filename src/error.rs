use std::{io, str::Utf8Error};

use thiserror::Error;

/// All possible errors returned by this library.
#[derive(Error, Debug)]
pub enum Error {
    /// An IO error happened on the underlying TCP stream.
    #[error(transparent)]
    IoError(#[from] io::Error),
    /// An error happened while decoding the data from Redis as UTF-8.
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),
    /// The parser implementation returned an error.
    #[error("Failed to parse the incoming Redis message.")]
    ParserError(#[from] crate::message::ParserError),
    /// You attempted to unsubscribe from a channel that you were not subscribed to.
    #[error("Not subscribed to the supplied channel.")]
    NotSubscribed,
    /// Zero bytes were read from the TCP socket: this is an IO error and is usually fatal.
    #[error("No bytes are read from the socket, socket is closed.")]
    ZeroBytesRead,
}

/// An wrapper around the standard [Result] type with [Error] aliased to this crate's error type.
///
/// [Result]: std::result::Result
/// [Error]: crate::error::Error
pub type Result<T> = std::result::Result<T, Error>;

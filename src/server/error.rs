use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("unknown command {0}")]
    UnknownCommand(String),
    #[error("connection issue")]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    ParseError(#[from] serde_resp::Error),
    #[error("response error")]
    ResponseError(String),
    #[error("empty response")]
    EmptyResponse,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

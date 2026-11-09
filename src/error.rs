use serde::{Deserialize, Serialize};
use std::fmt;
use tonic::{Code, Status};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcError {
    code: i32,
    message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    storage_fault: Option<bool>,
}

impl RpcError {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code: code as i32,
            message: message.into(),
            storage_fault: None,
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    pub fn corrupt(message: impl Into<String>) -> Self {
        Self::new(Code::DataLoss, message)
    }

    pub fn status(&self) -> Status {
        Status::new(Code::from_i32(self.code), self.message.clone())
    }

    pub fn is_storage_fault(&self) -> bool {
        self.storage_fault.unwrap_or(matches!(
            Code::from_i32(self.code),
            Code::Internal | Code::DataLoss
        ))
    }

    pub fn alarm_corrupt() -> Self {
        Self {
            code: Code::DataLoss as i32,
            message: "etcdserver: database corruption".into(),
            storage_fault: Some(false),
        }
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", Code::from_i32(self.code), self.message)
    }
}

impl std::error::Error for RpcError {}

impl From<rusqlite::Error> for RpcError {
    fn from(error: rusqlite::Error) -> Self {
        Self::new(
            Code::Internal,
            format!("SQLite application storage: {error}"),
        )
    }
}

impl From<std::io::Error> for RpcError {
    fn from(error: std::io::Error) -> Self {
        Self::new(Code::Internal, format!("node filesystem: {error}"))
    }
}

impl From<serde_json::Error> for RpcError {
    fn from(error: serde_json::Error) -> Self {
        Self::corrupt(format!("application encoding: {error}"))
    }
}

impl From<prost::DecodeError> for RpcError {
    fn from(error: prost::DecodeError) -> Self {
        Self::corrupt(format!("stored protobuf encoding: {error}"))
    }
}

pub type Result<T> = std::result::Result<T, RpcError>;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("rocksdb: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("codec: {0}")]
    Codec(#[from] bincode::Error),

    #[error("column family {0} is missing")]
    MissingColumnFamily(&'static str),

    #[error("message {0} not found")]
    NotFound(String),
}

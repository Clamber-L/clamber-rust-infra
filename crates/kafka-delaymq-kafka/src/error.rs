#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("kafka: {0}")]
    Rdkafka(#[from] rdkafka::error::KafkaError),

    #[error("codec: {0}")]
    Codec(#[from] bincode::Error),

    #[error("delivery to {topic} timed out")]
    DeliveryTimeout { topic: String },

    #[error("message is missing its payload wrapper")]
    EmptyRecord,
}

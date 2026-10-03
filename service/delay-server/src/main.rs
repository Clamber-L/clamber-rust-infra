mod config;
mod server;

use anyhow::Result;
use config::ServerConfig;
use server::DelayServer;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    // TODO: 从配置文件/环境变量读取，现在是占位值
    let config = ServerConfig::new(
        std::env::var("DELAY_BROKERS").unwrap_or_else(|_| "localhost:9092".into()),
        std::env::var("DELAY_TOPIC").unwrap_or_else(|_| "delay-topic".into()),
        vec![0],
        std::env::var("DELAY_DATA_DIR").unwrap_or_else(|_| "./data/delay".into()),
    );

    tracing::info!(
        brokers = %config.brokers,
        topic = %config.delay_topic,
        partitions = ?config.partitions,
        data_dir = %config.data_dir.display(),
        "starting delay server"
    );

    DelayServer::new(config)?.run().await
}

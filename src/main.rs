use streamforge::broker::server::Server;
use streamforge::log::LogConfig;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt().init();
    let config = LogConfig::default();
    let server = Server::new(std::path::Path::new("./data"), config);
    server.run("0.0.0.0:9876").await
}

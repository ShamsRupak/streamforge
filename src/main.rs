use std::path::PathBuf;
use streamforge::broker::server::Server;
use streamforge::log::LogConfig;

// ── CLI argument parser ───────────────────────────────────────────────────────

struct Args {
    port: u16,
    data_dir: PathBuf,
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().collect();
    let mut port = 9876u16;
    let mut data_dir = PathBuf::from("./data");

    let mut i = 1usize;
    while i < raw.len() {
        match raw[i].as_str() {
            "--port" => {
                i += 1;
                port = raw
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| {
                        eprintln!("error: --port requires a valid u16");
                        std::process::exit(1);
                    });
            }
            "--data-dir" => {
                i += 1;
                data_dir = match raw.get(i) {
                    Some(p) => PathBuf::from(p),
                    None => {
                        eprintln!("error: --data-dir requires a path");
                        std::process::exit(1);
                    }
                };
            }
            "--help" | "-h" => {
                println!(
                    "StreamForge — Rust event streaming engine\n\
                     \n\
                     Usage: streamforge [OPTIONS]\n\
                     \n\
                     Options:\n\
                       --port <PORT>         TCP port to listen on  [default: 9876]\n\
                       --data-dir <PATH>     Directory for log segments [default: ./data]\n\
                       -h, --help            Print this help message"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("error: unknown argument '{}'", other);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    Args { port, data_dir }
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt().init();

    let args = parse_args();
    let addr = format!("0.0.0.0:{}", args.port);

    tracing::info!(
        "StreamForge broker listening on {} (data: {})",
        addr,
        args.data_dir.display()
    );

    let config = LogConfig::default();
    let server = Server::new(&args.data_dir, config);
    server.run(&addr).await
}

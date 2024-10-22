use std::{
    fs,
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
};

use tokio::signal::unix::SignalKind;
use tracing::info;

pub mod cluster;
pub mod node;

/// Used to store marker files of "used" ports to avoid confilcts
///
/// Please make sure the path `$TMP_DIR/restate_test_ports` is deleted
/// before starting tests
const PORTS_POOL: &str = "restate_test_ports";

pub fn shutdown() -> impl Future<Output = &'static str> {
    let mut interrupt = tokio::signal::unix::signal(SignalKind::interrupt())
        .expect("failed to register signal handler");
    let mut terminate = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to register signal handler");

    async move {
        let signal = tokio::select! {
            _ = interrupt.recv() => "SIGINT",
            _ = terminate.recv() => "SIGTERM",
        };

        info!(%signal, "Received signal, starting cluster shutdown.");
        signal
    }
}

pub fn random_socket_address() -> io::Result<SocketAddr> {
    let base_path = std::env::temp_dir().join(PORTS_POOL);
    // this can happen repeatedly but it's a test so it's okay
    fs::create_dir_all(&base_path)?;

    loop {
        let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
        let socket_addr = listener.local_addr()?;

        let port_file = base_path.join(socket_addr.port().to_string());
        match fs::metadata(&port_file) {
            Ok(_) => {
                // file exists! try again
                continue;
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                // touch the file
                fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .create_new(true)
                    .write(true)
                    .open(port_file)?;

                return Ok(socket_addr);
            }
            Err(err) => return Err(err),
        }
    }
}

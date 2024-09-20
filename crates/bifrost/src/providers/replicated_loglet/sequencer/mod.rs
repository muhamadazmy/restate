use restate_core::network::NetworkError;
use restate_types::nodes_config::NodesConfigError;

mod node;

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("invalid node-set configuration")]
    InvalidNodeSet,

    #[error(transparent)]
    Network(#[from] NetworkError),
    #[error(transparent)]
    NodeConfig(#[from] NodesConfigError),
}

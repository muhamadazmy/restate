use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use restate_core::network::NetworkError;
use restate_types::{
    logs::LogletOffset,
    net::log_server::{LogletInfo, Store},
    GenerationalNodeId,
};

/// LogletHandler trait abstracts the log-server loglet interface. One of possible implementations
/// is a grpc client to running log server
#[async_trait::async_trait]
pub trait NodeClient {
    async fn enqueue_store(&self, msg: Store) -> Result<(), NetworkError>;
    async fn enqueue_get_loglet_info(&self) -> Result<(), NetworkError>;
}

#[derive(derive_more::Deref)]
pub(crate) struct Node<C> {
    #[deref]
    client: C,
    pub state: Option<NodeState>,
}

#[derive(derive_more::Debug, derive_more::Deref, derive_more::DerefMut)]
#[debug("{:?}", inner.keys().collect::<Vec<&GenerationalNodeId>>())]
pub(crate) struct NodeSet<C> {
    #[deref]
    #[deref_mut]
    inner: HashMap<GenerationalNodeId, Node<C>>,
    // todo: extra information in the node set for the replication
    // policy
}

impl<C> NodeSet<C> {
    pub fn sealed_nodes(&self) -> usize {
        self.values()
            .filter(|n| matches!(n.state, Some(NodeState { sealed, .. }) if sealed))
            .count()
    }

    pub fn has_state(&self) -> usize {
        self.values().filter(|n| n.state.is_some()).count()
    }

    // alive returns an iterator over all nodes that has sent
    // message in the past <since> duration
    pub fn alive(&self, since: Duration) -> impl Iterator<Item = &Node<C>> {
        let now = Instant::now();
        self.values().filter(move |n| {
            matches!(
                n.state,
                Some(NodeState {
                    last_response_time,
                    ..
                })
                if now.duration_since(last_response_time) < since
            )
        })
    }

    // return an iterator over all nodes that has no message sent since the given duration
    pub fn lagging(
        &self,
        since: Duration,
    ) -> impl Iterator<Item = (&GenerationalNodeId, &Node<C>)> {
        let now = Instant::now();
        self.iter().filter(move |(_, n)| {
            n.state.is_none()
                || matches!(
                    n.state,
                    Some(NodeState {
                        last_response_time,
                        ..
                    })
                    if now.duration_since(last_response_time) >= since
                )
        })
    }
}

impl<C> From<HashMap<GenerationalNodeId, C>> for NodeSet<C> {
    fn from(value: HashMap<GenerationalNodeId, C>) -> Self {
        let inner: HashMap<GenerationalNodeId, Node<C>> = value
            .into_iter()
            .map(|(key, client)| {
                (
                    key,
                    Node {
                        client,
                        state: None,
                    },
                )
            })
            .collect();

        Self { inner }
    }
}

#[derive(Debug, Clone)]
pub struct NodeState {
    pub last_response_time: Instant,
    pub local_tail: LogletOffset,
    pub sealed: bool,
    pub trim_point: LogletOffset,
    pub inflight_tail: LogletOffset,
}

impl From<LogletInfo> for NodeState {
    fn from(value: LogletInfo) -> Self {
        Self {
            last_response_time: Instant::now(),
            local_tail: value.local_tail,
            sealed: value.sealed,
            trim_point: value.trim_point,
            inflight_tail: value.local_tail,
        }
    }
}

impl NodeState {
    pub(crate) fn update(&mut self, info: LogletInfo) {
        self.last_response_time = Instant::now();
        self.local_tail = info.local_tail;
        self.sealed = info.sealed;
        self.trim_point = info.trim_point;

        if info.local_tail > self.inflight_tail {
            self.inflight_tail = info.local_tail;
        }
    }
}

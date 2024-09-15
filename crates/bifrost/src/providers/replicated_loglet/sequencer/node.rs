use std::{
    collections::HashMap,
    sync::{
        atomic::{self, Ordering},
        Arc,
    },
    time::Duration,
};

use super::{
    worker::{NodeWorker, NodeWorkerHandle, SendPermit},
    Error, SequencerGlobalState,
};
use restate_core::{network::NetworkError, TaskCenter};
use restate_types::{
    logs::{LogletOffset, SequenceNumber},
    net::log_server::{LogletInfo, Store},
    time::MillisSinceEpoch,
    GenerationalNodeId,
};

/// LogletHandler trait abstracts the log-server loglet interface. One of possible implementations
/// is a grpc client to running log server
#[async_trait::async_trait]
pub trait NodeClient {
    async fn enqueue_store(&self, msg: Store) -> Result<(), NetworkError>;
    async fn enqueue_get_loglet_info(&self) -> Result<(), NetworkError>;
}

#[derive(Debug, Default)]
pub struct NodeStatus {
    // todo: this should be monotonic
    last_response_time: atomic::AtomicU64,
}

impl NodeStatus {
    pub(crate) fn touch(&self) {
        // update value with latest timestamp
        self.last_response_time
            .store(MillisSinceEpoch::now().into(), Ordering::Relaxed);
    }

    pub fn last_response_time(&self) -> MillisSinceEpoch {
        self.last_response_time.load(Ordering::Relaxed).into()
    }

    pub fn duration_since_last_update(&self) -> Duration {
        // last_response_time should be monotonic
        self.last_response_time().elapsed()
    }
}

struct NodeInner<C> {
    client: C,
    state: NodeStatus,
}

pub struct Node<C> {
    inner: Arc<NodeInner<C>>,
}

impl<C> Clone for Node<C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C> Node<C> {
    fn new(client: C) -> Self {
        Self {
            inner: Arc::new(NodeInner {
                client,
                state: NodeStatus::default(),
            }),
        }
    }
    pub fn client(&self) -> &C {
        &self.inner.client
    }

    pub fn status(&self) -> &NodeStatus {
        &self.inner.state
    }
}

#[derive(derive_more::Debug, derive_more::Deref, derive_more::DerefMut)]
#[debug("{:?}", inner.keys().collect::<Vec<&GenerationalNodeId>>())]
/// Nodes represents the entire cluster of nodes available. This can be shared with
/// multiple Sequencers
pub struct Nodes<C> {
    #[deref]
    #[deref_mut]
    inner: HashMap<GenerationalNodeId, Node<C>>,
}

impl<C> From<HashMap<GenerationalNodeId, C>> for Nodes<C> {
    fn from(value: HashMap<GenerationalNodeId, C>) -> Self {
        let inner: HashMap<GenerationalNodeId, Node<C>> = value
            .into_iter()
            .map(|(key, client)| (key, Node::new(client)))
            .collect();

        Self { inner }
    }
}

#[derive(Debug, Clone)]
pub struct LogletNodeStatus {
    pub sealed: bool,
    pub local_tail: LogletOffset,
    pub inflight_tail: LogletOffset,
    pub trim_point: LogletOffset,
}

impl Default for LogletNodeStatus {
    fn default() -> Self {
        Self {
            sealed: false,
            local_tail: LogletOffset::INVALID,
            inflight_tail: LogletOffset::INVALID,
            trim_point: LogletOffset::INVALID,
        }
    }
}

impl LogletNodeStatus {
    pub fn update(&mut self, info: &LogletInfo) {
        self.sealed = info.sealed;
        self.local_tail = info.local_tail;
        self.trim_point = info.trim_point;
    }
}

#[derive(Debug, derive_more::Deref, derive_more::DerefMut)]
pub(crate) struct LogletNode {
    #[deref]
    #[deref_mut]
    pub status: LogletNodeStatus,
    pub worker: NodeWorkerHandle,
}

/// NodeSet is a subset of Nodes that is maintained internally with each sequencer
#[derive(derive_more::Deref, derive_more::DerefMut, Debug)]
pub(crate) struct NodeSet {
    inner: HashMap<GenerationalNodeId, LogletNode>,
}

impl NodeSet {
    /// creates the node set and start the appenders
    pub fn start<C>(
        tc: &TaskCenter,
        node_set: Vec<GenerationalNodeId>,
        nodes: &Nodes<C>,
        shared: Arc<SequencerGlobalState>,
    ) -> Result<Self, super::Error>
    where
        C: NodeClient + Send + Sync + 'static,
    {
        let mut inner = HashMap::new();
        for id in node_set {
            let node = match nodes.get(&id) {
                Some(node) => node.clone(),
                None => return Err(super::Error::InvalidNodeSet),
            };

            let worker = NodeWorker::start(tc, node, 100, Arc::clone(&shared))?;

            inner.insert(
                id,
                LogletNode {
                    status: LogletNodeStatus::default(),
                    worker,
                },
            );
        }

        Ok(Self { inner })
    }
}

impl NodeSet {
    pub fn sealed_nodes(&self) -> usize {
        self.values()
            .filter(|n| matches!(n.status, LogletNodeStatus { sealed, .. } if sealed))
            .count()
    }

    /// get spread of nodes that can satisfy the provided inflight_tail.
    pub async fn spread(&self, _inflight_tail: &LogletOffset) -> Result<Spread, Error> {
        // todo: apply replication policy here and find all nodes that
        // we need to replicate to.

        let mut spread = Spread::default();
        // for now we do ALL!
        for (id, node) in self.iter() {
            let permit = node
                .worker
                .reserve()
                .await
                .map_err(|_| Error::TermporaryUnavailable(id.clone()))?;

            spread.insert(id.clone(), permit);
        }

        Ok(spread)
    }
}

pub(crate) type Spread<'a> = HashMap<GenerationalNodeId, SendPermit<'a>>;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::{
        atomic::{self, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::Stream;
use node::{LogletNodeStatus, NodeClient, NodeSet, Nodes};
use tokio::sync::{mpsc, oneshot};
use worker::{Batch, Payload};

use restate_core::{cancellation_token, ShutdownError, TaskCenter, TaskKind};
use restate_types::{
    logs::{LogletOffset, Lsn, Record, SequenceNumber, TailState},
    net::log_server::{LogletInfo, Status, Stored},
    replicated_loglet::ReplicatedLogletId,
    GenerationalNodeId,
};

use crate::loglet::LogletCommit;

mod node;
mod worker;

const SUBSCRIPTION_STREAM_SIZE: usize = 64;
const NODE_HEARTBEAT: Duration = Duration::from_secs(3);
const NODE_MAX_LAST_RESPONSE_DURATION: Duration = Duration::from_secs(2);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("malformed batch")]
    MalformedBatch,
    #[error("invalid node set")]
    InvalidNodeSet,
    #[error("node {0} queue is full")]
    TermporaryUnavailable(GenerationalNodeId),
    #[error(transparent)]
    ShutdownError(#[from] ShutdownError),
}

/// internal commands sent over the [`SequencerHandler`] to sequencer main loop
struct Command<Q, A> {
    request: Q,
    sender: oneshot::Sender<A>,
}

impl<Q, A> Command<Q, A> {
    fn from_request(request: Q) -> (oneshot::Receiver<A>, Self) {
        let (sender, receiver) = oneshot::channel();
        (receiver, Self { request, sender })
    }
}

#[derive(derive_more::Deref)]
struct Signal<S> {
    peer: GenerationalNodeId,
    #[deref]
    signal: S,
}

impl<S> Signal<S> {
    fn new(peer: GenerationalNodeId, signal: S) -> Self {
        Self { peer, signal }
    }
}

/// Internal commands to the sequencer main loop. This is exclusively used
/// by the SequencerHandler
enum Commands {
    ClusterState(Command<(), HashMap<GenerationalNodeId, LogletNodeStatus>>),
    /// executed commands
    EnqueueBatch(Command<Arc<[Record]>, Result<LogletCommit, Error>>),
}

// Internal possible signals that can be received async from log server
enum Signals {
    Stored(Signal<Stored>),
    LogletInfo(Signal<LogletInfo>),
}

impl Signals {
    fn peer(&self) -> &GenerationalNodeId {
        match self {
            Self::Stored(signal) => &signal.peer,
            Self::LogletInfo(signal) => &signal.peer,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SequencerHandle {
    /// internal commands channel.
    commands: mpsc::Sender<Commands>,
    /// internal signal channels. Signals are responses that are received
    /// async and sent from log server nodes.
    /// they are separated from the commands, in case we need to block command
    /// processing until a set of responses has been received
    signals: mpsc::Sender<Signals>,
}

pub(crate) struct Receiver {
    commands: mpsc::Receiver<Commands>,
    signals: mpsc::Receiver<Signals>,
}

impl SequencerHandle {
    pub(crate) fn pair() -> (SequencerHandle, Receiver) {
        // todo: the size of the channel should be 1
        let (commands_sender, commands_receiver) = mpsc::channel::<Commands>(1);
        let (signals_sender, signals_receiver) = mpsc::channel::<Signals>(64);
        (
            SequencerHandle {
                commands: commands_sender,
                signals: signals_sender,
            },
            Receiver {
                commands: commands_receiver,
                signals: signals_receiver,
            },
        )
    }

    pub fn watch_tail(&self) -> futures::stream::BoxStream<'static, TailState<LogletOffset>> {
        unimplemented!()
    }

    pub async fn cluster_state(
        &self,
    ) -> Result<HashMap<GenerationalNodeId, LogletNodeStatus>, ShutdownError> {
        let (receiver, command) = Command::from_request(());
        self.commands
            .send(Commands::ClusterState(command))
            .await
            .map_err(|_| ShutdownError)?;

        receiver.await.map_err(|_| ShutdownError)
    }

    pub async fn enqueue_batch(&self, payloads: Arc<[Record]>) -> Result<LogletCommit, Error> {
        let (receiver, command) = Command::from_request(payloads);
        self.commands
            .send(Commands::EnqueueBatch(command))
            .await
            .map_err(|_| ShutdownError)?;

        receiver.await.map_err(|_| ShutdownError)?
    }

    pub async fn signal_stored(
        &self,
        peer: impl Into<GenerationalNodeId>,
        payloads: Stored,
    ) -> Result<(), ShutdownError> {
        let signal = Signal::new(peer.into(), payloads);
        self.signals
            .send(Signals::Stored(signal))
            .await
            .map_err(|_| ShutdownError)
    }

    pub async fn signal_loglet_info(
        &self,
        peer: impl Into<GenerationalNodeId>,
        payloads: LogletInfo,
    ) -> Result<(), ShutdownError> {
        let signal = Signal::new(peer.into(), payloads);
        self.signals
            .send(Signals::LogletInfo(signal))
            .await
            .map_err(|_| ShutdownError)
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send + Sync>>;

/// part of state that is shared between multiple appenders
#[derive(Debug)]
struct SequencerGlobalState {
    node_id: GenerationalNodeId,
    loglet_id: ReplicatedLogletId,
    global_committed_tail: atomic::AtomicU32,
}

impl SequencerGlobalState {
    pub fn committed_tail(&self) -> LogletOffset {
        LogletOffset::new(self.global_committed_tail.load(Ordering::Relaxed))
    }

    pub fn set_committed_tail(&self, tail: LogletOffset) {
        self.global_committed_tail
            .fetch_max(tail.into(), Ordering::Relaxed);
    }
}

/// Sequencer inner state machine
///
/// this holds for example, the replica set (log servers)
/// information about global tail, etc...
#[derive(Debug)]
struct SequencerInner<C> {
    nodes: node::Nodes<C>,
    node_set: NodeSet,
    sealed: bool,
    handle: SequencerHandle,
    offset: Lsn,
    inflight_tail: LogletOffset,
    shared: Arc<SequencerGlobalState>,
    batches: VecDeque<Batch>,
}

pub struct Sequencer;
impl Sequencer {
    pub fn start<C>(
        task_center: &TaskCenter,
        node_id: GenerationalNodeId,
        loglet_id: ReplicatedLogletId,
        offset: Lsn,
        nodes: Nodes<C>,
        node_set: Vec<GenerationalNodeId>,
    ) -> Result<SequencerHandle, Error>
    where
        C: node::NodeClient + Send + Sync + 'static,
    {
        // - register for all potential response streams from the log-server(s).

        // create a command channel to be used by the sequencer handler. The handler then can be used
        // to call and execute commands on the sequencer directly
        let (handle, commands) = SequencerHandle::pair();

        let sequencer = SequencerInner::new(
            &task_center,
            node_id,
            loglet_id,
            nodes,
            node_set,
            handle.clone(),
            offset,
        )?;

        task_center.spawn_unmanaged(
            TaskKind::SystemService,
            "leader-sequencer",
            None,
            sequencer.run(commands),
        )?;

        Ok(handle)
    }
}

impl<C> SequencerInner<C>
where
    C: NodeClient + Send + Sync + 'static,
{
    fn new(
        task_center: &TaskCenter,
        node_id: GenerationalNodeId,
        loglet_id: ReplicatedLogletId,
        nodes: Nodes<C>,
        node_set: Vec<GenerationalNodeId>,
        handle: SequencerHandle,
        offset: Lsn,
    ) -> Result<Self, Error> {
        // shared state with appenders
        let shared = Arc::new(SequencerGlobalState {
            node_id,
            loglet_id,
            global_committed_tail: atomic::AtomicU32::new(LogletOffset::OLDEST.into()),
        });

        // build the node set
        let node_set = NodeSet::start(task_center, node_set, &nodes, Arc::clone(&shared))?;

        //
        Ok(Self {
            nodes,
            node_set,
            handle,
            offset,
            inflight_tail: LogletOffset::OLDEST,
            shared,
            sealed: false,
            batches: VecDeque::default(),
        })
    }

    async fn run(mut self, mut input: Receiver) {
        let shutdown = cancellation_token();

        let mut heartbeat = tokio::time::interval(NODE_HEARTBEAT);

        // enter main state machine loop
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    break;
                },
                Some(command) = input.commands.recv() => {
                    self.process_command(command).await;
                }
                Some(signal) = input.signals.recv() => {
                    self.process_signal(signal);
                }
                // we rely on the fact that the tick is resolved immediately
                // on first call to send a hearbeat to all nodes in the cluster
                _ = heartbeat.tick() => {
                    self.heartbeat().await;
                }
            }
        }
    }

    async fn heartbeat(&self) {
        for id in self.node_set.keys() {
            if let Some(node) = self.nodes.get(id) {
                if node.status().duration_since_last_update() < NODE_MAX_LAST_RESPONSE_DURATION {
                    continue;
                }
                // otherwise send a heartbeat and wait for response
                if let Err(_) = node.client().enqueue_get_loglet_info().await {
                    tracing::warn!(node=%id, "failed to send get-loglet-info to node")
                }
            }
        }
    }

    /// process calls from the SequencerHandler.
    async fn process_command(&mut self, command: Commands) {
        match command {
            Commands::ClusterState(command) => {
                let Command { sender, .. } = command;
                if let Err(_) = sender.send(self.cluster_state()) {}
            }
            Commands::EnqueueBatch(command) => {
                let Command { request, sender } = command;

                let _ = sender.send(self.enqueue_batch(request).await);
            }
        }
    }

    /// process calls from the SequencerHandler.
    fn process_signal(&mut self, signal: Signals) {
        if let Some(node) = self.nodes.get(signal.peer()) {
            node.status().touch();
        }

        match signal {
            Signals::Stored(signal) => {
                self.signal_stored(signal);
            }
            Signals::LogletInfo(signal) => {
                self.signal_loglet_info(signal);
            }
        }
    }

    fn cluster_state(&self) -> HashMap<GenerationalNodeId, LogletNodeStatus> {
        self.node_set
            .iter()
            .map(|(id, node)| (id.clone(), node.status.clone()))
            .collect()
    }

    async fn enqueue_batch(&mut self, records: Arc<[Record]>) -> Result<LogletCommit, Error> {
        if self.sealed {
            // todo: (question) do we return a sealed loglet commit, or error.
            return Ok(LogletCommit::sealed());
        }

        // - create a partial store
        let store = Arc::new(Payload {
            first_offset: self.inflight_tail,
            records,
        });

        // - compute the new inflight_tail
        let inflight_tail = store.inflight_tail().ok_or(Error::MalformedBatch)?;

        // - get the next spread of nodes from the node set that can satisfy this inflight_tail
        let spread = self.node_set.spread(&inflight_tail).await?;

        let (commit, resolver) = LogletCommit::later();
        let mut node_set = HashSet::with_capacity(spread.len());

        for (node_id, permit) in spread {
            node_set.insert(node_id);
            permit.send(Arc::downgrade(&store));
        }

        // we need to update the inflight tail of the spread
        for id in node_set.iter() {
            if let Some(node) = self.node_set.get_mut(id) {
                node.status.inflight_tail = inflight_tail;
            }
        }

        // - create a batch that will be eventually resolved after we
        // receive all "Stored" commands
        let batch = Batch {
            spread: node_set,
            payload: store,
            resolver,
        };

        self.inflight_tail = inflight_tail;
        self.batches.push_back(batch);

        Ok(commit)
    }

    fn signal_stored(&mut self, stored: Signal<Stored>) {
        match stored.status {
            Status::Sealed | Status::Sealing => {
                self.sealed = true;
                if let Some(node) = self.node_set.get_mut(&stored.peer) {
                    node.sealed = true;
                }
                // reject all batches
                // todo: need revision, this might not be always correct
                for batch in self.batches.drain(..) {
                    batch.resolver.sealed();
                }
                return;
            }
            Status::Ok => {
                if let Some(node) = self.node_set.get_mut(&stored.peer) {
                    // update node local tail.
                    if stored.local_tail > node.status.local_tail {
                        node.status.local_tail = stored.local_tail;
                    }
                }
                // store succeeded
            }
            _ => {
                todo!()
            }
        }

        let mut resolve = 0;
        for batch in self.batches.iter_mut() {
            // if the node local tail is smaller that the batch inflight tail then
            // we can safely break from this loop, since every next batch will
            // have a higher inflight tail.
            // it's also safe to unwrap the inflight_tail here since it won't
            // even be in the batches queue if the inflight tail was invalid
            let batch_inflight_tail = batch.payload.inflight_tail().unwrap();
            if stored.local_tail < batch_inflight_tail {
                break;
            }

            batch.spread.remove(&stored.peer);
            if batch.spread.is_empty() {
                // we can safely resolve this batch now since all nodes
                // in the spread has responded.
                resolve += 1;
            } else {
                break;
            }
        }

        for batch in self.batches.drain(..resolve) {
            let inflight_tail = batch.payload.inflight_tail().unwrap();
            self.shared.set_committed_tail(inflight_tail);
            // todo: (azmy) we probably need to do a release here
            batch.resolver.offset(inflight_tail);
        }
    }

    fn signal_loglet_info(&mut self, signal: Signal<LogletInfo>) {
        let Signal {
            peer,
            signal: loglet_info,
        } = signal;

        if loglet_info.sealed {
            self.sealed = true;
            // todo: finish sealing ?
        }

        // update last response time
        if let Some(node) = self.nodes.get(&peer) {
            node.status().touch();
        }

        self.node_set.entry(peer).and_modify(|node| {
            node.update(&loglet_info);
        });
    }
}

/// todo: (azmy) build actual tests this is just experiments
/// over interactions with log-server
#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use restate_core::{network::NetworkError, TaskCenterBuilder, TaskKind};
    use restate_types::{
        logs::{LogletOffset, Lsn, Record, SequenceNumber},
        net::log_server::{LogServerResponseHeader, LogletInfo, Status, Store, Stored},
        GenerationalNodeId,
    };

    use crate::setup_panic_handler;

    use super::{
        node::{NodeClient, NodeSet},
        SequencerHandle, SequencerInner,
    };

    struct MockNodeClient {
        id: GenerationalNodeId,
        handle: SequencerHandle,
    }

    #[async_trait::async_trait]
    impl NodeClient for MockNodeClient {
        async fn enqueue_store(&self, msg: Store) -> Result<(), NetworkError> {
            // directly respond with stored answer
            let local_tail = msg.last_offset().unwrap() + 1;
            self.handle
                .signal_stored(
                    self.id.clone(),
                    Stored {
                        header: LogServerResponseHeader {
                            local_tail: local_tail,
                            sealed: false,
                            status: Status::Ok,
                        },
                    },
                )
                .await?;

            Ok(())
        }

        async fn enqueue_get_loglet_info(&self) -> Result<(), NetworkError> {
            self.handle
                .signal_loglet_info(
                    self.id.clone(),
                    LogletInfo {
                        header: LogServerResponseHeader {
                            local_tail: LogletOffset::OLDEST,
                            sealed: false,
                            status: Status::Ok,
                        },
                        trim_point: LogletOffset::OLDEST,
                    },
                )
                .await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_setup() {
        setup_panic_handler();
        let tc = TaskCenterBuilder::default_for_tests().build().unwrap();

        let (handle, input) = SequencerHandle::pair();

        let mut nodes = HashMap::new();

        let node1 = MockNodeClient {
            id: GenerationalNodeId::new(2, 1),
            handle: handle.clone(),
        };

        let node2 = MockNodeClient {
            id: GenerationalNodeId::new(3, 1),
            handle: handle.clone(),
        };

        let node_set = vec![node1.id.clone(), node2.id.clone()];

        nodes.insert(node1.id.clone(), node1);
        nodes.insert(node2.id.clone(), node2);

        let sequencer = SequencerInner::new(
            &tc,
            GenerationalNodeId::new(1, 1),
            100.into(),
            nodes.into(),
            node_set.into(),
            handle.clone(),
            Lsn::OLDEST,
        )
        .unwrap();

        tc.spawn_unmanaged(
            TaskKind::SystemService,
            "sequencer",
            None,
            sequencer.run(input),
        )
        .unwrap();

        let records = vec![Record::from("hello world"), Record::from("hello human")];
        let resolved = handle.enqueue_batch(Arc::from(records)).await.unwrap();

        println!("waiting for resolved commit");
        let tail = tokio::time::timeout(Duration::from_secs(2), resolved)
            .await
            .unwrap()
            .unwrap();

        let expected = LogletOffset::new(3);
        assert_eq!(tail, expected);

        let state = handle.cluster_state().await.unwrap();
        for state in state.values() {
            assert_eq!(state.local_tail, expected);
        }
    }
}

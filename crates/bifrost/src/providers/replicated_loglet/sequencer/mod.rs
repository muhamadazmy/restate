use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use futures::Stream;
use node::{NodeClient, NodeSet, NodeState};
use tokio::sync::{mpsc, oneshot};

use restate_core::{cancellation_token, network::Incoming, ShutdownError, TaskCenter, TaskKind};
use restate_types::{
    logs::{LogId, LogletOffset, Lsn, Record, SequenceNumber, TailState},
    net::log_server::{LogletInfo, Stored},
    GenerationalNodeId,
};

use crate::loglet::LogletCommit;

mod node;

const SUBSCRIPTION_STREAM_SIZE: usize = 64;
const NODE_HEARTBEAT: Duration = Duration::from_secs(2);

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

struct Signal<S> {
    peer: GenerationalNodeId,
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
    ClusterState(Command<(), HashMap<GenerationalNodeId, Option<NodeState>>>),
    /// executed commands
    EnqueueBatch(Command<Arc<[Record]>, Result<LogletCommit, ShutdownError>>),
}

// Internal possible signals that can be received async from log server
enum Signals {
    Stored(Signal<Stored>),
    LogletInfo(Signal<LogletInfo>),
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
    ) -> Result<HashMap<GenerationalNodeId, Option<NodeState>>, ShutdownError> {
        let (receiver, command) = Command::from_request(());
        self.commands
            .send(Commands::ClusterState(command))
            .await
            .map_err(|_| ShutdownError)?;

        receiver.await.map_err(|_| ShutdownError)
    }

    pub async fn enqueue_batch(
        &self,
        payloads: Arc<[Record]>,
    ) -> Result<LogletCommit, ShutdownError> {
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

/// Sequencer inner state machine
///
/// this holds for example, the replica set (log servers)
/// information about global tail, etc...
#[derive(Debug)]
struct SequencerInner<C> {
    node_set: node::NodeSet<C>,
    sealed: bool,
    handle: SequencerHandle,
    offset: Lsn,
    global_committed_tail: LogletOffset,
}

pub struct Sequencer;
impl Sequencer {
    pub fn start<C>(
        task_center: &TaskCenter,
        _loglet_id: LogId,
        offset: Lsn,
        node_set: HashMap<GenerationalNodeId, C>,
    ) -> Result<SequencerHandle, ShutdownError>
    where
        C: node::NodeClient + Send + Sync + 'static,
    {
        // - register for all potential response streams from the log-server(s).

        // create a command channel to be used by the sequencer handler. The handler then can be used
        // to call and execute commands on the sequencer directly
        let (handle, commands) = SequencerHandle::pair();

        let sequencer = SequencerInner::new(node_set.into(), handle.clone(), offset);

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
    C: NodeClient,
{
    fn new(node_set: NodeSet<C>, handle: SequencerHandle, offset: Lsn) -> Self {
        Self {
            node_set,
            handle,
            offset,
            global_committed_tail: LogletOffset::OLDEST,
            sealed: false,
        }
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
                    self.process_command(command);
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
        for (id, node) in self.node_set.lagging(NODE_HEARTBEAT) {
            if let Err(_) = node.enqueue_get_loglet_info().await {
                tracing::warn!(node=%id, "failed to send get-loglet-info to node")
            }
        }
    }

    /// process calls from the SequencerHandler.
    fn process_command(&mut self, command: Commands) {
        match command {
            Commands::ClusterState(command) => {
                let Command { sender, .. } = command;
                if let Err(_) = sender.send(self.cluster_state()) {}
            }
            Commands::EnqueueBatch(command) => {
                let Command { request, sender } = command;

                let _ = sender.send(self.enqueue_batch(request));
            }
        }
    }

    /// process calls from the SequencerHandler.
    fn process_signal(&mut self, signal: Signals) {
        match signal {
            Signals::Stored(signal) => {
                self.signal_stored(signal);
            }
            Signals::LogletInfo(signal) => {
                self.signal_loglet_info(signal);
            }
        }
    }

    fn cluster_state(&self) -> HashMap<GenerationalNodeId, Option<NodeState>> {
        self.node_set
            .iter()
            .map(|(id, node)| (id.clone(), node.state.clone()))
            .collect()
    }

    fn enqueue_batch(&mut self, records: Arc<[Record]>) -> Result<LogletCommit, ShutdownError> {
        unimplemented!()
    }

    fn signal_stored(&mut self, stored: Signal<Stored>) {
        unimplemented!()
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

        self.node_set.entry(peer).and_modify(|node| {
            if let Some(ref mut state) = node.state {
                state.update(loglet_info);
            } else {
                node.state = Some(loglet_info.into());
            }
        });
    }
}

/// todo: (azmy) build actual tests this is just experiments
/// over interactions with log-server
#[cfg(test)]
mod test {
    use std::{collections::HashMap, time::Duration};

    use restate_core::{
        network::{Incoming, NetworkError},
        TaskCenter, TaskCenterBuilder, TaskKind,
    };
    use restate_types::{
        logs::{LogletOffset, Lsn, SequenceNumber},
        net::log_server::{LogServerResponseHeader, LogletInfo, Status, Store, Stored},
        protobuf::node,
        GenerationalNodeId,
    };

    use super::{node::NodeClient, SequencerHandle, SequencerInner};

    struct MockNodeClient {
        id: GenerationalNodeId,
        handle: SequencerHandle,
    }

    #[async_trait::async_trait]
    impl NodeClient for MockNodeClient {
        async fn enqueue_store(&self, msg: Store) -> Result<(), NetworkError> {
            // directly respond with stored answer
            self.handle
                .signal_stored(
                    self.id.clone(),
                    Stored {
                        header: LogServerResponseHeader {
                            local_tail: LogletOffset::OLDEST,
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
        let tc = TaskCenterBuilder::default_for_tests().build().unwrap();

        let (handle, input) = SequencerHandle::pair();

        let mut node_set = HashMap::new();

        let node1 = MockNodeClient {
            id: GenerationalNodeId::new(1, 1),
            handle: handle.clone(),
        };

        let node2 = MockNodeClient {
            id: GenerationalNodeId::new(2, 1),
            handle: handle.clone(),
        };

        node_set.insert(node1.id.clone(), node1);
        node_set.insert(node2.id.clone(), node2);

        let sequencer = SequencerInner::new(node_set.into(), handle.clone(), Lsn::OLDEST);
        tc.spawn_unmanaged(
            TaskKind::SystemService,
            "sequencer",
            None,
            sequencer.run(input),
        )
        .unwrap();

        // test that sequencer is processing requests
        loop {
            let state = handle.cluster_state().await.unwrap();
            for state in state.values() {
                if state.is_none() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    break;
                }
            }
            return;
        }
    }
}

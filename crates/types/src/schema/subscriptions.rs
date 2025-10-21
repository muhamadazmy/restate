// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;

use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

use crate::config::KafkaClusterOptions;
use crate::identifiers::SubscriptionId;
use crate::invocation::{VirtualObjectHandlerType, WorkflowHandlerType};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum Source {
    Kafka { cluster: String, topic: String },
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::Kafka { cluster, topic, .. } => {
                write!(f, "kafka://{cluster}/{topic}")
            }
        }
    }
}

impl PartialEq<&str> for Source {
    fn eq(&self, other: &&str) -> bool {
        self.to_string().as_str() == *other
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(from = "serde_hacks::Sink", into = "serde_hacks::Sink")]
pub enum Sink {
    Invocation {
        event_invocation_target_template: EventInvocationTargetTemplate,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventInvocationTargetTemplate {
    Service {
        name: String,
        handler: String,
    },
    VirtualObject {
        name: String,
        handler: String,
        handler_ty: VirtualObjectHandlerType,
    },
    Workflow {
        name: String,
        handler: String,
        handler_ty: WorkflowHandlerType,
    },
}

impl fmt::Display for Sink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Sink::Invocation {
                event_invocation_target_template:
                    EventInvocationTargetTemplate::Service { name, handler, .. },
            }
            | Sink::Invocation {
                event_invocation_target_template:
                    EventInvocationTargetTemplate::VirtualObject { name, handler, .. },
            }
            | Sink::Invocation {
                event_invocation_target_template:
                    EventInvocationTargetTemplate::Workflow { name, handler, .. },
            } => {
                write!(f, "service://{name}/{handler}")
            }
        }
    }
}

impl PartialEq<&str> for Sink {
    fn eq(&self, other: &&str) -> bool {
        self.to_string().as_str() == *other
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ValidationError {
    #[error("missing required option '{name}'")]
    Missing { name: &'static str },
    #[error("invalid option '{name}'. Reason: {reason}")]
    Invalid {
        name: &'static str,
        reason: &'static str,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Subscription {
    source: Source,
    sink: Sink,
    metadata: HashMap<String, String>,
}

impl Subscription {
    fn new(source: Source, sink: Sink, metadata: HashMap<String, String>) -> Self {
        Self {
            source,
            sink,
            metadata,
        }
    }

    /// Creates a new subscription from cluster config
    pub fn from_cluster(
        cluster: &KafkaClusterOptions,
        source: Source,
        sink: Sink,
        metadata: HashMap<String, String>,
    ) -> Result<Self, ValidationError> {
        // Retrieve the cluster option and merge them with subscription metadata
        let mut cluster_options = cluster.additional_options.clone();

        // override cluster options with subscription metadata
        for (key, value) in metadata {
            cluster_options.insert(key, value);
        }

        if cluster_options.contains_key("enable.auto.commit") {
            warn!(
                "The configuration option enable.auto.commit should not be set and it will be ignored."
            );
        }

        if cluster_options.contains_key("enable.auto.offset.store") {
            warn!(
                "The configuration option enable.auto.offset.store should not be set and it will be ignored."
            );
        }

        if !cluster_options.contains_key("group.id") {
            return Err(ValidationError::Missing { name: "group.id" });
        }

        // Set client.id if unset
        if !cluster_options.contains_key("client.id") {
            cluster_options.insert("client.id".to_string(), "restate".to_string());
        }

        Ok(Self::new(source, sink, cluster_options))
    }

    pub fn group_id(&self) -> Option<&str> {
        self.metadata.get("group.id").map(|s| s.as_str())
    }

    /// Get the subscription deterministic id
    /// The id is deterministic over the kafka consumer group.id and topic
    ///
    /// Panics if group.id is not set.
    pub fn id(&self) -> SubscriptionId {
        let group_id = self.group_id().expect("kafka group.id is set");
        let Source::Kafka { topic, .. } = &self.source;
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        hasher.update(group_id.as_bytes());
        hasher.update(topic.as_bytes());

        SubscriptionId::from(hasher.digest128())
    }

    pub fn source(&self) -> &Source {
        &self.source
    }

    pub fn sink(&self) -> &Sink {
        &self.sink
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata
    }
}

pub enum ListSubscriptionFilter {
    ExactMatchSink(String),
    ExactMatchSource(String),
}

impl ListSubscriptionFilter {
    pub fn matches(&self, sub: &Subscription) -> bool {
        match self {
            ListSubscriptionFilter::ExactMatchSink(sink) => sub.sink == sink.as_str(),
            ListSubscriptionFilter::ExactMatchSource(source) => sub.source == source.as_str(),
        }
    }
}

pub trait SubscriptionResolver {
    fn get_subscription(&self, id: SubscriptionId) -> Option<Subscription>;

    fn list_subscriptions(&self, filters: &[ListSubscriptionFilter]) -> Vec<Subscription>;
}

mod serde_hacks {
    use super::*;

    /// Specialized version of [super::service::ServiceType]
    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    pub enum EventReceiverServiceType {
        VirtualObject,
        Workflow,
        Service,
    }

    // TODO(slinkydeveloper) this migration will be executed in 1.5, together with the new schema registry
    //  we should be able to remove it when we remove the old schema registry migration
    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub enum Sink {
        // Could not use the Rust built-in deprecated feature because some macros will fail with it and won't apply the #[allow(deprecated)] :(
        #[serde(rename = "Service")]
        DeprecatedService {
            name: String,
            handler: String,
            ty: EventReceiverServiceType,
        },
        Invocation {
            event_invocation_target_template: EventInvocationTargetTemplate,
        },
    }

    impl From<Sink> for super::Sink {
        fn from(value: Sink) -> Self {
            match value {
                Sink::DeprecatedService {
                    name,
                    handler,
                    ty: EventReceiverServiceType::Service,
                } => Self::Invocation {
                    event_invocation_target_template: EventInvocationTargetTemplate::Service {
                        name,
                        handler,
                    },
                },
                Sink::DeprecatedService {
                    name,
                    handler,
                    ty: EventReceiverServiceType::VirtualObject,
                } => Self::Invocation {
                    event_invocation_target_template:
                        EventInvocationTargetTemplate::VirtualObject {
                            name,
                            handler,
                            handler_ty: VirtualObjectHandlerType::Exclusive,
                        },
                },
                Sink::DeprecatedService {
                    name,
                    handler,
                    ty: EventReceiverServiceType::Workflow,
                } => Self::Invocation {
                    event_invocation_target_template: EventInvocationTargetTemplate::Workflow {
                        name,
                        handler,
                        handler_ty: WorkflowHandlerType::Workflow,
                    },
                },
                Sink::Invocation {
                    event_invocation_target_template,
                    ..
                } => Self::Invocation {
                    event_invocation_target_template,
                },
            }
        }
    }

    impl From<super::Sink> for Sink {
        fn from(value: super::Sink) -> Self {
            match value {
                super::Sink::Invocation {
                    event_invocation_target_template,
                } => Self::Invocation {
                    event_invocation_target_template,
                },
            }
        }
    }
}

#[cfg(feature = "test-util")]
pub mod mocks {

    use super::*;

    impl Subscription {
        pub fn mock() -> Self {
            Subscription {
                source: Source::Kafka {
                    cluster: "my-cluster".to_string(),
                    topic: "my-topic".to_string(),
                },
                sink: Sink::Invocation {
                    event_invocation_target_template: EventInvocationTargetTemplate::Service {
                        name: "MySvc".to_string(),
                        handler: "MyMethod".to_string(),
                    },
                },
                metadata: Default::default(),
            }
        }
    }
}

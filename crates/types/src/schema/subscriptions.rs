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

use restate_encoding::BilrostAs;
use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

use super::Schema;
use crate::config::IngressOptions;
use crate::errors::GenericError;
use crate::identifiers::SubscriptionId;
use crate::invocation::{VirtualObjectHandlerType, WorkflowHandlerType};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default, BilrostAs)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[bilrost_as(dto::Source)]
pub enum Source {
    #[default]
    Unknown,
    Kafka {
        cluster: String,
        topic: String,
    },
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::Unknown => {
                write!(f, "unknown://")
            }
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

/// Specialized version of [super::service::ServiceType]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, bilrost::Enumeration)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum EventReceiverServiceType {
    Service = 0,
    VirtualObject = 1,
    Workflow = 2,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default, BilrostAs)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[bilrost_as(dto::Sink)]
pub enum Sink {
    #[default]
    Unknown,
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
            Sink::Unknown => {
                write!(f, "unknown://")
            }
            Sink::DeprecatedService { name, handler, .. } => {
                write!(f, "service://{name}/{handler}")
            }
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, bilrost::Message)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Subscription {
    #[bilrost(1)]
    id: SubscriptionId,
    #[bilrost(2)]
    source: Source,
    #[bilrost(3)]
    sink: Sink,
    #[bilrost(4)]
    metadata: HashMap<String, String>,
}

impl Subscription {
    pub fn new(
        id: SubscriptionId,
        source: Source,
        sink: Sink,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            id,
            source,
            sink,
            metadata,
        }
    }

    pub fn id(&self) -> SubscriptionId {
        self.id
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

impl SubscriptionResolver for Schema {
    fn get_subscription(&self, id: SubscriptionId) -> Option<Subscription> {
        self.subscriptions.get(&id).cloned()
    }

    fn list_subscriptions(&self, filters: &[ListSubscriptionFilter]) -> Vec<Subscription> {
        self.subscriptions
            .values()
            .filter(|sub| {
                for f in filters {
                    if !f.matches(sub) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }
}

pub trait SubscriptionValidator {
    type Error: Into<GenericError>;

    fn validate(&self, subscription: Subscription) -> Result<Subscription, Self::Error>;
}

#[derive(Debug, thiserror::Error)]
#[error("invalid option '{name}'. Reason: {reason}")]
pub struct ValidationError {
    name: &'static str,
    reason: &'static str,
}

impl SubscriptionValidator for IngressOptions {
    type Error = ValidationError;

    fn validate(&self, mut subscription: Subscription) -> Result<Subscription, Self::Error> {
        // Retrieve the cluster option and merge them with subscription metadata
        let Source::Kafka { cluster, .. } = subscription.source() else {
            return Err(ValidationError {
                name: "source",
                reason: "unknown subscription source",
            });
        };

        let cluster_options = &self.get_kafka_cluster(cluster).ok_or(ValidationError {
            name: "source",
            reason: "specified cluster in the source URI does not exist. Make sure it is defined in the KafkaOptions",
        })?.additional_options;

        if cluster_options.contains_key("enable.auto.commit")
            || subscription.metadata().contains_key("enable.auto.commit")
        {
            warn!(
                "The configuration option enable.auto.commit should not be set and it will be ignored."
            );
        }
        if cluster_options.contains_key("enable.auto.offset.store")
            || subscription
                .metadata()
                .contains_key("enable.auto.offset.store")
        {
            warn!(
                "The configuration option enable.auto.offset.store should not be set and it will be ignored."
            );
        }

        // Set the group.id if unset
        if !(cluster_options.contains_key("group.id")
            || subscription.metadata().contains_key("group.id"))
        {
            let group_id = subscription.id().to_string();

            subscription
                .metadata_mut()
                .insert("group.id".to_string(), group_id);
        }

        // Set client.id if unset
        if !(cluster_options.contains_key("client.id")
            || subscription.metadata().contains_key("client.id"))
        {
            subscription
                .metadata_mut()
                .insert("client.id".to_string(), "restate".to_string());
        }

        Ok(subscription)
    }
}

mod dto {
    use crate::invocation::{VirtualObjectHandlerType, WorkflowHandlerType};

    use super::EventReceiverServiceType;

    #[derive(bilrost::Message)]
    struct KafkaSource {
        cluster: String,
        topic: String,
    }

    #[derive(bilrost::Message)]
    pub(super) struct Source {
        #[bilrost(1)]
        kafka: Option<KafkaSource>,
    }

    impl From<&super::Source> for Source {
        fn from(value: &super::Source) -> Self {
            match value {
                super::Source::Unknown => Self { kafka: None },
                super::Source::Kafka { cluster, topic } => Self {
                    kafka: Some(KafkaSource {
                        cluster: cluster.clone(),
                        topic: topic.clone(),
                    }),
                },
            }
        }
    }

    impl From<Source> for super::Source {
        fn from(value: Source) -> Self {
            match value.kafka {
                None => Self::Unknown,
                Some(KafkaSource { cluster, topic }) => Self::Kafka { cluster, topic },
            }
        }
    }

    #[derive(bilrost::Message)]
    struct DeprecatedService {
        name: String,
        handler: String,
        ty: EventReceiverServiceType,
    }

    #[derive(bilrost::Message)]
    struct Invocation {
        ty: EventReceiverServiceType,
        name: String,
        handler: String,
        shared: bool,
    }

    impl From<super::EventInvocationTargetTemplate> for Invocation {
        fn from(value: super::EventInvocationTargetTemplate) -> Self {
            use super::EventInvocationTargetTemplate as Template;
            match value {
                Template::Service { name, handler } => Invocation {
                    ty: EventReceiverServiceType::Service,
                    name,
                    handler,
                    shared: true,
                },
                Template::VirtualObject {
                    name,
                    handler,
                    handler_ty,
                } => Invocation {
                    ty: EventReceiverServiceType::VirtualObject,
                    name,
                    handler,
                    shared: handler_ty == VirtualObjectHandlerType::Shared,
                },
                Template::Workflow {
                    name,
                    handler,
                    handler_ty,
                } => Invocation {
                    ty: EventReceiverServiceType::Workflow,
                    name,
                    handler,
                    shared: handler_ty == WorkflowHandlerType::Shared,
                },
            }
        }
    }

    impl From<Invocation> for super::EventInvocationTargetTemplate {
        fn from(value: Invocation) -> Self {
            let Invocation {
                ty,
                name,
                handler,
                shared,
            } = value;

            match ty {
                EventReceiverServiceType::Service => Self::Service { name, handler },
                EventReceiverServiceType::VirtualObject => Self::VirtualObject {
                    name,
                    handler,
                    handler_ty: if shared {
                        VirtualObjectHandlerType::Shared
                    } else {
                        VirtualObjectHandlerType::Exclusive
                    },
                },
                EventReceiverServiceType::Workflow => Self::Workflow {
                    name,
                    handler,
                    handler_ty: if shared {
                        WorkflowHandlerType::Shared
                    } else {
                        WorkflowHandlerType::Workflow
                    },
                },
            }
        }
    }

    #[derive(bilrost::Oneof)]
    enum SinkInner {
        Unknown,
        #[bilrost(1)]
        DeprecatedService(DeprecatedService),
        #[bilrost(2)]
        Invocation(Invocation),
    }

    #[derive(bilrost::Message)]
    pub(super) struct Sink(#[bilrost(oneof(1, 2))] SinkInner);

    impl From<&super::Sink> for Sink {
        fn from(value: &super::Sink) -> Self {
            let inner = match value {
                super::Sink::Unknown => SinkInner::Unknown,
                super::Sink::DeprecatedService { name, handler, ty } => {
                    SinkInner::DeprecatedService(DeprecatedService {
                        name: name.clone(),
                        handler: handler.clone(),
                        ty: ty.clone(),
                    })
                }
                super::Sink::Invocation {
                    event_invocation_target_template,
                } => SinkInner::Invocation(event_invocation_target_template.clone().into()),
            };

            Self(inner)
        }
    }

    impl From<Sink> for super::Sink {
        fn from(value: Sink) -> Self {
            match value.0 {
                SinkInner::Unknown => Self::Unknown,
                SinkInner::DeprecatedService(deprecated) => Self::DeprecatedService {
                    name: deprecated.name,
                    handler: deprecated.handler,
                    ty: deprecated.ty,
                },
                SinkInner::Invocation(invocation) => Self::Invocation {
                    event_invocation_target_template: invocation.into(),
                },
            }
        }
    }
}

#[cfg(feature = "test-util")]
pub mod mocks {
    use std::str::FromStr;

    use super::*;

    impl Subscription {
        pub fn mock() -> Self {
            let id = SubscriptionId::from_str("sub_15VqmTOnXH3Vv2pl5HOG7Ua")
                .expect("stable valid subscription id");
            Subscription {
                id,
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

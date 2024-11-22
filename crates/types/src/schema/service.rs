// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::invocation_target::InvocationTargetMetadata;
use super::Schema;
use crate::identifiers::{DeploymentId, ServiceRevision};
use crate::invocation::{
    InvocationTargetType, ServiceType, VirtualObjectHandlerType, WorkflowHandlerType,
};
use crate::schema::openapi::ServiceOpenAPI;
use arc_swap::ArcSwapOption;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ServiceMetadata {
    /// # Name
    ///
    /// Fully qualified name of the service
    pub name: String,

    pub handlers: Vec<HandlerMetadata>,

    pub ty: ServiceType,

    /// # Documentation
    ///
    /// Documentation of the service, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    /// # Metadata
    ///
    /// Additional service metadata, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    /// # Deployment Id
    ///
    /// Deployment exposing the latest revision of the service.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub deployment_id: DeploymentId,

    /// # Revision
    ///
    /// Latest revision of the service.
    pub revision: ServiceRevision,

    /// # Public
    ///
    /// If true, the service can be invoked through the ingress.
    /// If false, the service can be invoked only from another Restate service.
    pub public: bool,

    /// # Idempotency retention
    ///
    /// The retention duration of idempotent requests for this service.
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub idempotency_retention: humantime::Duration,

    /// # Workflow completion retention
    ///
    /// The retention duration of workflows. Only available on workflow services.
    #[serde(
        with = "serde_with::As::<Option<serde_with::DisplayFromStr>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub workflow_completion_retention: Option<humantime::Duration>,

    /// # Inactivity timeout
    ///
    /// This timer guards against stalled service/handler invocations. Once it expires,
    /// Restate triggers a graceful termination by asking the service invocation to
    /// suspend (which preserves intermediate progress).
    ///
    /// The 'abort timeout' is used to abort the invocation, in case it doesn't react to
    /// the request to suspend.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    ///
    /// This overrides the default inactivity timeout set in invoker options.
    #[serde(
        with = "serde_with::As::<Option<serde_with::DisplayFromStr>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub inactivity_timeout: Option<humantime::Duration>,

    /// # Abort timeout
    ///
    /// This timer guards against stalled service/handler invocations that are supposed to
    /// terminate. The abort timeout is started after the 'inactivity timeout' has expired
    /// and the service/handler invocation has been asked to gracefully terminate. Once the
    /// timer expires, it will abort the service/handler invocation.
    ///
    /// This timer potentially **interrupts** user code. If the user code needs longer to
    /// gracefully terminate, then this value needs to be set accordingly.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    ///
    /// This overrides the default abort timeout set in invoker options.
    #[serde(
        with = "serde_with::As::<Option<serde_with::DisplayFromStr>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub abort_timeout: Option<humantime::Duration>,
}

// This type is used only for exposing the handler metadata, and not internally. See [ServiceAndHandlerType].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum HandlerMetadataType {
    Exclusive,
    Shared,
    Workflow,
}

impl From<InvocationTargetType> for Option<HandlerMetadataType> {
    fn from(value: InvocationTargetType) -> Self {
        match value {
            InvocationTargetType::Service => None,
            InvocationTargetType::VirtualObject(h_ty) => match h_ty {
                VirtualObjectHandlerType::Exclusive => Some(HandlerMetadataType::Exclusive),
                VirtualObjectHandlerType::Shared => Some(HandlerMetadataType::Shared),
            },
            InvocationTargetType::Workflow(h_ty) => match h_ty {
                WorkflowHandlerType::Workflow => Some(HandlerMetadataType::Workflow),
                WorkflowHandlerType::Shared => Some(HandlerMetadataType::Shared),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct HandlerMetadata {
    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ty: Option<HandlerMetadataType>,

    /// # Documentation
    ///
    /// Documentation of the handler, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    /// # Metadata
    ///
    /// Additional handler metadata, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    /// # Human readable input description
    ///
    /// If empty, no schema was provided by the user at discovery time.
    pub input_description: String,

    /// # Human readable output description
    ///
    /// If empty, no schema was provided by the user at discovery time.
    pub output_description: String,

    /// # Input JSON Schema
    ///
    /// JSON Schema of the handler input
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_json_schema: Option<serde_json::Value>,

    /// # Output JSON Schema
    ///
    /// JSON Schema of the handler output
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_json_schema: Option<serde_json::Value>,
}

/// This API will return services registered by the user.
pub trait ServiceMetadataResolver {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata>;

    fn resolve_latest_service_openapi(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<serde_json::Value>;

    fn resolve_latest_service_type(&self, service_name: impl AsRef<str>) -> Option<ServiceType>;

    fn list_services(&self) -> Vec<ServiceMetadata>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerSchemas {
    pub target_meta: InvocationTargetMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceSchemas {
    pub revision: ServiceRevision,
    pub handlers: HashMap<String, HandlerSchemas>,
    pub ty: ServiceType,
    pub location: ServiceLocation,
    pub idempotency_retention: Duration,
    pub workflow_completion_retention: Option<Duration>,
    pub inactivity_timeout: Option<Duration>,
    pub abort_timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    /// This is a cache for the computed value of ServiceOpenAPI
    #[serde(skip)]
    pub service_openapi_cache: Arc<ArcSwapOption<ServiceOpenAPI>>,
}

impl ServiceSchemas {
    pub fn as_service_metadata(&self, name: String) -> ServiceMetadata {
        ServiceMetadata {
            name,
            handlers: self
                .handlers
                .iter()
                .map(|(h_name, h_schemas)| HandlerMetadata {
                    name: h_name.clone(),
                    ty: h_schemas.target_meta.target_ty.into(),
                    documentation: h_schemas.documentation.clone(),
                    metadata: h_schemas.metadata.clone(),
                    input_description: h_schemas.target_meta.input_rules.to_string(),
                    output_description: h_schemas.target_meta.output_rules.to_string(),
                    input_json_schema: h_schemas.target_meta.input_rules.json_schema(),
                    output_json_schema: h_schemas.target_meta.output_rules.json_schema(),
                })
                .collect(),
            ty: self.ty,
            documentation: self.documentation.clone(),
            metadata: self.metadata.clone(),
            deployment_id: self.location.latest_deployment,
            revision: self.revision,
            public: self.location.public,
            idempotency_retention: self.idempotency_retention.into(),
            workflow_completion_retention: self.workflow_completion_retention.map(Into::into),
            inactivity_timeout: self.inactivity_timeout.map(Into::into),
            abort_timeout: self.abort_timeout.map(Into::into),
        }
    }

    pub fn openapi_spec(&self, name: &str) -> serde_json::Value {
        let service_openapi = {
            let cached_openapi = self.service_openapi_cache.load();
            if let Some(result) = cached_openapi.as_ref() {
                Arc::clone(result)
            } else {
                // Not present, compute it!
                let computed = Arc::new(ServiceOpenAPI::infer(name, self.ty, &self.handlers));
                self.service_openapi_cache.store(Some(computed.clone()));
                computed
            }
        };

        service_openapi.to_openapi_contract(name, self.documentation.as_deref(), self.revision)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceLocation {
    pub latest_deployment: DeploymentId,
    pub public: bool,
}

impl ServiceMetadataResolver for Schema {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        let name = service_name.as_ref();
        self.use_service_schema(name, |service_schemas| {
            service_schemas.as_service_metadata(name.to_owned())
        })
    }

    fn resolve_latest_service_openapi(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<serde_json::Value> {
        let name = service_name.as_ref();
        self.use_service_schema(name, |service_schemas| service_schemas.openapi_spec(name))
    }

    fn resolve_latest_service_type(&self, service_name: impl AsRef<str>) -> Option<ServiceType> {
        self.use_service_schema(service_name.as_ref(), |service_schemas| service_schemas.ty)
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        self.services
            .iter()
            .map(|(service_name, service_schemas)| {
                service_schemas.as_service_metadata(service_name.clone())
            })
            .collect()
    }
}

#[cfg(feature = "test-util")]
#[allow(dead_code)]
pub mod test_util {
    use super::*;

    use serde_json::Value;
    use std::collections::HashMap;

    #[derive(Debug, Default, Clone)]
    pub struct MockServiceMetadataResolver(HashMap<String, ServiceMetadata>);

    impl MockServiceMetadataResolver {
        pub fn add(&mut self, service_metadata: ServiceMetadata) {
            self.0
                .insert(service_metadata.name.clone(), service_metadata);
        }
    }

    impl ServiceMetadataResolver for MockServiceMetadataResolver {
        fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
            self.0.get(service_name.as_ref()).cloned()
        }

        fn resolve_latest_service_openapi(&self, _service_name: impl AsRef<str>) -> Option<Value> {
            Some(Value::Null)
        }

        fn resolve_latest_service_type(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<ServiceType> {
            self.0.get(service_name.as_ref()).map(|c| c.ty)
        }

        fn list_services(&self) -> Vec<ServiceMetadata> {
            self.0.values().cloned().collect()
        }
    }

    impl ServiceMetadata {
        pub fn mock_service(
            name: impl AsRef<str>,
            handlers: impl IntoIterator<Item = impl AsRef<str>>,
        ) -> Self {
            Self {
                name: name.as_ref().to_string(),
                handlers: handlers
                    .into_iter()
                    .map(|s| HandlerMetadata {
                        name: s.as_ref().to_string(),
                        ty: None,
                        documentation: None,
                        metadata: Default::default(),
                        input_description: "any".to_string(),
                        output_description: "any".to_string(),
                        input_json_schema: None,
                        output_json_schema: None,
                    })
                    .collect(),
                ty: ServiceType::Service,
                documentation: None,
                metadata: Default::default(),
                deployment_id: Default::default(),
                revision: 0,
                public: true,
                idempotency_retention: Duration::from_secs(60).into(),
                workflow_completion_retention: None,
                inactivity_timeout: None,
                abort_timeout: None,
            }
        }

        pub fn mock_virtual_object(
            name: impl AsRef<str>,
            handlers: impl IntoIterator<Item = impl AsRef<str>>,
        ) -> Self {
            Self {
                name: name.as_ref().to_string(),
                handlers: handlers
                    .into_iter()
                    .map(|s| HandlerMetadata {
                        name: s.as_ref().to_string(),
                        ty: Some(HandlerMetadataType::Exclusive),
                        documentation: None,
                        metadata: Default::default(),
                        input_description: "any".to_string(),
                        output_description: "any".to_string(),
                        input_json_schema: None,
                        output_json_schema: None,
                    })
                    .collect(),
                ty: ServiceType::VirtualObject,
                documentation: None,
                metadata: Default::default(),
                deployment_id: Default::default(),
                revision: 0,
                public: true,
                idempotency_retention: Duration::from_secs(60).into(),
                workflow_completion_retention: None,
                inactivity_timeout: None,
                abort_timeout: None,
            }
        }
    }
}

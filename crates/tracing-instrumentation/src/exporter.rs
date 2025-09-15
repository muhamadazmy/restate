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
use std::sync::OnceLock;

use dashmap::{DashMap, Entry};
use http::HeaderMap;
use opentelemetry::{Key, KeyValue, StringValue, Value};
use opentelemetry_otlp::{
    Protocol, SpanExporter as OTelSpanExporter, WithExportConfig, WithHttpConfig, WithTonicConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use opentelemetry_semantic_conventions::attribute::{RPC_SERVICE, SERVICE_NAME};
use restate_serde_util::SerdeableHeaderHashMap;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, ClientTlsConfig};

use restate_types::GenerationalNodeId;

/// `RPC_SERVICE` is used to override `service.name` on the `SpanBuilder`
const RPC_SERVICE_KEY: Key = Key::from_static_str(RPC_SERVICE);

static GLOBAL_NODE_ID: OnceLock<GenerationalNodeId> = OnceLock::new();

pub fn set_global_node_id(node_id: GenerationalNodeId) {
    GLOBAL_NODE_ID
        .set(node_id)
        .expect("Global NodeId is not set")
}

#[derive(Debug, Clone)]
pub enum ExporterBuilder {
    Tonic {
        metadata: MetadataMap,
        channel: Channel,
        protocol: Protocol,
    },
    Http {
        client: reqwest::Client,
        headers: HashMap<String, String>,
        protocol: Protocol,
        endpoint: http::Uri,
    },
}

impl ExporterBuilder {
    pub fn new(
        endpoint: impl AsRef<str>,
        tracing_headers: SerdeableHeaderHashMap,
    ) -> Result<Self, super::Error> {
        // Parse it as a URI and extract the scheme.
        let endpoint = endpoint.as_ref();

        let uri = endpoint
            .parse::<http::Uri>()
            .map_err(|e| super::bad_endpoint(format!("{endpoint}: URI: {e}")))?;

        let scheme = uri
            .scheme()
            .ok_or_else(|| super::bad_endpoint(format!("{endpoint}: no scheme")))?
            .to_string();

        // Tokenize the scheme on '+' to determine the type of exporter.
        let mut scheme_tokens: Vec<&str> = scheme.split('+').collect();
        scheme_tokens.sort();

        enum Transport {
            Tonic, // gRPC
            Http,  // HTTP(s)
        }

        // Map specific token combinations to ultimate endpoint scheme, exporter
        // transport, and exporter protocol.
        let (final_scheme, use_transport, use_protocol) = match scheme_tokens.as_slice() {
            ["http"] => ("http", Transport::Tonic, Protocol::Grpc),
            ["https"] => ("https", Transport::Tonic, Protocol::Grpc),
            ["grpc"] => ("http", Transport::Tonic, Protocol::Grpc),
            ["grpc", "otlp"] => ("http", Transport::Tonic, Protocol::Grpc),
            ["http", "otlp"] => ("http", Transport::Http, Protocol::HttpBinary),
            ["https", "otlp"] => ("https", Transport::Http, Protocol::HttpBinary),
            ["http", "otlp", "proto"] => ("http", Transport::Http, Protocol::HttpBinary),
            ["https", "otlp", "proto"] => ("https", Transport::Http, Protocol::HttpBinary),
            ["http", "json", "otlp"] => ("http", Transport::Http, Protocol::HttpJson),
            ["https", "json", "otlp"] => ("https", Transport::Http, Protocol::HttpJson),
            _ => return Err(super::bad_endpoint(format!("{endpoint}: invalid scheme"))),
        };

        // Reconstruct the endpoint with the ultimate scheme from above.
        let endpoint = http::uri::Builder::from(uri)
            .scheme(final_scheme)
            .build()
            .map_err(|e| super::bad_endpoint(format!("rebuild endpoint: {e}")))?;

        // Build the exporter as specified.
        let builder = match use_transport {
            Transport::Tonic => {
                let metadata_headers: MetadataMap =
                    MetadataMap::from_headers(HeaderMap::from_iter(HashMap::from(tracing_headers)));
                let channel = Channel::builder(endpoint)
                    .tls_config(ClientTlsConfig::new().with_native_roots())
                    .map_err(|err| super::Error::Other(err.into()))?
                    .connect_lazy();

                ExporterBuilder::Tonic {
                    metadata: metadata_headers,
                    channel,
                    protocol: use_protocol,
                }
            }
            Transport::Http => {
                let client = reqwest::Client::builder()
                    .use_rustls_tls() // match with_tonic with_tls_config
                    .tls_built_in_root_certs(true) // match with_tonic with_tls_config
                    .build()
                    .map_err(|e| super::bad_endpoint(format!("build HTTP client: {e}")))?;
                let string_headers: HashMap<String, String> = HashMap::from(tracing_headers)
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k.as_str().into(),
                            String::from_utf8_lossy(v.as_bytes()).into(),
                        )
                    })
                    .collect();
                ExporterBuilder::Http {
                    client,
                    headers: string_headers,
                    protocol: use_protocol,
                    endpoint,
                }
            }
        };

        Ok(builder)
    }

    pub fn build(&self) -> Result<opentelemetry_otlp::SpanExporter, super::Error> {
        match self {
            ExporterBuilder::Tonic {
                metadata,
                channel,
                protocol,
            } => Ok(OTelSpanExporter::builder()
                .with_tonic()
                .with_channel(channel.clone())
                .with_metadata(metadata.clone())
                .with_protocol(*protocol)
                .build()
                .map_err(|e| super::bad_endpoint(format!("build gRPC exporter: {e}")))?),

            ExporterBuilder::Http {
                client,
                headers,
                protocol,
                endpoint,
            } => Ok(OTelSpanExporter::builder()
                .with_http()
                .with_http_client(client.clone())
                .with_protocol(*protocol)
                .with_headers(headers.clone())
                .with_endpoint(endpoint.to_string())
                .build()
                .map_err(|e| super::bad_endpoint(format!("build HTTP exporter: {e}")))?),
        }
    }
}
/// `UserServiceModifierSpanExporter` wraps a `opentelemetry::sdk::trace::SpanExporter` in order to allow mutating
/// the service name which is within the resource field. As this field is set during export,
/// we are forced to intercept the export
#[derive(Debug)]
pub(crate) struct UserServiceModifierSpanExporter {
    resource: Resource,
    exporters: DashMap<StringValue, opentelemetry_otlp::SpanExporter>,
    builder: ExporterBuilder,
}

impl UserServiceModifierSpanExporter {
    pub(crate) fn new(
        endpoint: impl AsRef<str>,
        tracing_headers: SerdeableHeaderHashMap,
    ) -> Result<Self, super::Error> {
        Ok(UserServiceModifierSpanExporter {
            resource: Resource::builder_empty().build(),
            exporters: DashMap::new(),
            builder: ExporterBuilder::new(endpoint, tracing_headers)?,
        })
    }
}

impl SpanExporter for UserServiceModifierSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let mut spans_by_service =
            HashMap::<StringValue, Vec<SpanData>>::with_capacity(batch.len());

        for span in batch {
            if let Some(KeyValue {
                value: Value::String(string_value),
                ..
            }) = span.attributes.iter().find(|kv| kv.key == RPC_SERVICE_KEY)
            {
                spans_by_service
                    .entry(string_value.clone())
                    .or_default()
                    .push(span);
            } else {
                spans_by_service
                    .entry(StringValue::from(""))
                    .or_default()
                    .push(span);
            }
        }

        for (service_name, batch) in spans_by_service.into_iter() {
            match self.exporters.entry(service_name) {
                Entry::Occupied(entry) => {
                    entry.get().export(batch).await?;
                }
                Entry::Vacant(entry) => {
                    let mut exporter = self
                        .builder
                        .build()
                        .map_err(|e| OTelSdkError::InternalFailure(e.to_string()))?;

                    let mut resource_builder = Resource::builder_empty().with_attributes(
                        self.resource
                            .iter()
                            .map(|(k, v)| KeyValue::new(k.clone(), v.clone())),
                    );

                    if entry.key().as_str() != "" {
                        resource_builder = resource_builder
                            .with_attribute(KeyValue::new(SERVICE_NAME, entry.key().clone()));
                    }

                    exporter.set_resource(&resource_builder.build());
                    let exporter = entry.insert(exporter);

                    exporter.export(batch).await?;
                }
            };
        }

        Ok(())
    }

    fn shutdown(&mut self) -> OTelSdkResult {
        for mut entry in self.exporters.iter_mut() {
            entry.value_mut().shutdown()?;
        }

        Ok(())
    }

    fn force_flush(&mut self) -> OTelSdkResult {
        for mut entry in self.exporters.iter_mut() {
            entry.value_mut().force_flush()?;
        }

        Ok(())
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.clone();
    }
}

#[cfg(not(feature = "service_per_crate"))]
pub(crate) use service_per_binary::RuntimeModifierSpanExporter;

#[cfg(feature = "service_per_crate")]
pub(crate) use service_per_crate::RuntimeModifierSpanExporter;

#[cfg(not(feature = "service_per_crate"))]
mod service_per_binary {
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::{
        Resource,
        error::OTelSdkResult,
        trace::{SpanData, SpanExporter},
    };
    use opentelemetry_semantic_conventions::attribute::SERVICE_NAME;

    use super::GLOBAL_NODE_ID;

    #[derive(Debug)]
    pub(crate) struct RuntimeModifierSpanExporter<E>
    where
        E: SpanExporter,
    {
        inner: E,
    }

    impl<E> RuntimeModifierSpanExporter<E>
    where
        E: SpanExporter,
    {
        pub fn new(inner: E) -> Self {
            Self { inner }
        }
    }

    impl<E> SpanExporter for RuntimeModifierSpanExporter<E>
    where
        E: SpanExporter,
    {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            self.inner.export(batch).await
        }

        fn force_flush(&mut self) -> OTelSdkResult {
            self.inner.force_flush()
        }

        fn set_resource(&mut self, resource: &Resource) {
            if GLOBAL_NODE_ID.get().is_some() {
                let node_id = GLOBAL_NODE_ID.get().expect("is initialized");

                let attributes = vec![
                    // sets the SERVICE_INSTANCE_ID
                    KeyValue::new(SERVICE_NAME, node_id.to_string()),
                ];

                let resources = Resource::builder()
                    .with_attributes(
                        resource
                            .iter()
                            .map(|(k, v)| KeyValue::new(k.clone(), v.clone())),
                    )
                    .with_attributes(attributes)
                    .build();

                self.inner.set_resource(&resources);
            }

            self.inner.set_resource(resource);
        }

        fn shutdown(&mut self) -> OTelSdkResult {
            self.inner.shutdown()
        }
    }
}

#[cfg(feature = "service_per_crate")]
mod service_per_crate {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use arc_swap::ArcSwap;
    use futures::future::BoxFuture;
    use opentelemetry::{Key, KeyValue, StringValue, Value, trace::TraceError};
    use opentelemetry_sdk::{
        Resource,
        export::trace::{SpanData, SpanExporter},
    };
    use opentelemetry_semantic_conventions::attribute::{
        CODE_NAMESPACE, SERVICE_INSTANCE_ID, SERVICE_NAME,
    };

    use super::GLOBAL_NODE_ID;

    const CODE_NAMESPACE_KEY: Key = Key::from_static_str(CODE_NAMESPACE);
    /// `UserServiceModifierSpanExporter` wraps a `opentelemetry::sdk::trace::SpanExporter` in order to allow mutating
    /// the service name which is within the resource field. As this field is set during export,
    /// we are forced to intercept the export
    #[derive(Debug)]
    pub(crate) struct RuntimeModifierSpanExporter<T> {
        exporter: Option<Arc<Mutex<T>>>,
        resource: ArcSwap<Resource>,
        injected: bool,
    }

    impl<T> RuntimeModifierSpanExporter<T> {
        pub(crate) fn new(inner: T) -> Self {
            RuntimeModifierSpanExporter {
                exporter: Some(Arc::new(Mutex::new(inner))),
                resource: ArcSwap::from_pointee(Resource::empty()),
                injected: false,
            }
        }
    }

    impl<T: SpanExporter + 'static> SpanExporter for RuntimeModifierSpanExporter<T> {
        fn export(
            &mut self,
            batch: Vec<SpanData>,
        ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
            let exporter = match &self.exporter {
                Some(exporter) => exporter.clone(),
                None => {
                    return Box::pin(std::future::ready(Err(TraceError::Other(
                        "exporter is already shut down".into(),
                    ))));
                }
            };

            if !self.injected && GLOBAL_NODE_ID.get().is_some() {
                self.injected = true;
                let resource = self.resource.load();
                let resource = resource.merge(&Resource::new(vec![KeyValue::new(
                    SERVICE_INSTANCE_ID,
                    GLOBAL_NODE_ID
                        .get()
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "UNKNOWN".to_owned()),
                )]));

                self.resource.store(Arc::new(resource));
            }

            let mut spans_by_service =
                HashMap::<Option<StringValue>, Vec<SpanData>>::with_capacity(batch.len());

            for span in batch {
                if let Some(KeyValue {
                    value: Value::String(string_value),
                    ..
                }) = span
                    .attributes
                    .iter()
                    .find(|kv| kv.key == CODE_NAMESPACE_KEY)
                {
                    let service = if string_value.as_str().starts_with("restate_") {
                        string_value
                            .as_str()
                            .split_once("::")
                            .map(|(prefix, _)| prefix.strip_prefix("restate_").unwrap().to_owned())
                            .map(StringValue::from)
                    } else {
                        None
                    };
                    spans_by_service.entry(service).or_default().push(span);
                } else {
                    spans_by_service.entry(None).or_default().push(span);
                }
            }

            let resource = self.resource.load();

            Box::pin(async move {
                for (service_name, batch) in spans_by_service.into_iter() {
                    {
                        let mut exporter_guard = match exporter.lock() {
                            Ok(exporter) => exporter,
                            Err(_) => {
                                return Err(TraceError::Other("exporter mutex is poisoned".into()));
                            }
                        };
                        match service_name {
                            None => exporter_guard.set_resource(&resource),
                            Some(service_name) => {
                                exporter_guard.set_resource(&resource.merge(&Resource::new(
                                    std::iter::once(KeyValue::new(SERVICE_NAME, service_name)),
                                )))
                            }
                        }
                        exporter_guard.export(batch)
                    }
                    .await?;
                }
                Ok(())
            })
        }

        fn shutdown(&mut self) {
            if let Some(exporter) = self.exporter.take() {
                // wait for any in-flight export to finish
                if let Ok(mut exporter) = exporter.lock() {
                    exporter.shutdown()
                }
            }
        }

        fn force_flush(
            &mut self,
        ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
            let exporter = match &self.exporter {
                Some(exporter) => exporter.clone(),
                None => {
                    return Box::pin(std::future::ready(Err(TraceError::Other(
                        "exporter is already shut down".into(),
                    ))));
                }
            };
            // wait for any in-flight export to finish
            let mut exporter_guard = match exporter.lock() {
                Ok(exporter_guard) => exporter_guard,
                Err(_) => {
                    return Box::pin(std::future::ready(Err(TraceError::Other(
                        "exporter mutex is poisoned".into(),
                    ))));
                }
            };
            let fut = exporter_guard.force_flush();
            Box::pin(fut)
        }

        fn set_resource(&mut self, resource: &Resource) {
            self.resource.store(Arc::new(resource.clone()))
        }
    }
}

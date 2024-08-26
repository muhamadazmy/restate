use core::fmt;
use std::{
    sync::{Arc, RwLock},
    time::SystemTime,
};

use once_cell::sync::Lazy;
use opentelemetry::{
    global::{BoxedSpan, BoxedTracer, ObjectSafeTracerProvider},
    trace::{self, noop::NoopTracerProvider, SpanId, SpanKind, TraceId, Tracer, TracerProvider},
    Context, InstrumentationLibrary, KeyValue,
};
use opentelemetry_semantic_conventions::attribute::{RPC_METHOD, RPC_SERVICE, RPC_SYSTEM};
use restate_types::{identifiers::InvocationId, invocation::InvocationTarget};

#[derive(Clone)]
pub struct InvocationTracerProvider {
    provider: Arc<dyn ObjectSafeTracerProvider + Send + Sync>,
}

impl fmt::Debug for InvocationTracerProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InvocationTracerProvider")
    }
}

impl InvocationTracerProvider {
    /// Create a new GlobalTracerProvider instance from a struct that implements `TracerProvider`.
    fn new<P, T, S>(provider: P) -> Self
    where
        S: trace::Span + Send + Sync + 'static,
        T: trace::Tracer<Span = S> + Send + Sync + 'static,
        P: trace::TracerProvider<Tracer = T> + Send + Sync + 'static,
    {
        InvocationTracerProvider {
            provider: Arc::new(provider),
        }
    }
}

impl trace::TracerProvider for InvocationTracerProvider {
    type Tracer = BoxedTracer;

    /// Create a tracer using the global provider.
    fn library_tracer(&self, library: Arc<InstrumentationLibrary>) -> Self::Tracer {
        BoxedTracer::new(self.provider.boxed_tracer(library))
    }
}

/// The global `Tracer` provider singleton.
static INVOCATION_TRACER_PROVIDER: Lazy<RwLock<InvocationTracerProvider>> =
    Lazy::new(|| RwLock::new(InvocationTracerProvider::new(NoopTracerProvider::new())));

/// Sets the given [`TracerProvider`] instance as the current global provider.
///
/// It returns the [`TracerProvider`] instance that was previously mounted as global provider
/// (e.g. [`NoopTracerProvider`] if a provider had not been set before).
///
/// [`TracerProvider`]: crate::trace::TracerProvider
pub fn set_tracer_provider<P, T, S>(new_provider: P) -> InvocationTracerProvider
where
    S: trace::Span + Send + Sync + 'static,
    T: trace::Tracer<Span = S> + Send + Sync + 'static,
    P: trace::TracerProvider<Tracer = T> + Send + Sync + 'static,
{
    let mut tracer_provider = INVOCATION_TRACER_PROVIDER
        .write()
        .expect("GLOBAL_TRACER_PROVIDER RwLock poisoned");
    std::mem::replace(
        &mut *tracer_provider,
        InvocationTracerProvider::new(new_provider),
    )
}

const INVOCATION_ID: &str = "restate.invocation.id";
const INVOCATION_TARGET: &str = "restate.invocation.target";

pub struct InvocationSpanBuilder<'a> {
    parent: Option<&'a Context>,
    inner: opentelemetry::trace::SpanBuilder,
    attributes: Vec<KeyValue>,
    tracer: BoxedTracer,

    start_time: Option<SystemTime>,
    span_id: Option<SpanId>,
    trace_id: Option<TraceId>,
}

impl<'a> InvocationSpanBuilder<'a> {
    pub fn new(
        parent_ctx: Option<&'a Context>,
        prefix: Option<&str>,
        invocation_id: &InvocationId,
        invocation_target: &InvocationTarget,
    ) -> InvocationSpanBuilder<'a> {
        let provider = INVOCATION_TRACER_PROVIDER
            .read()
            .expect("TRACER_PROVIDER RwLock poisoned")
            .clone();

        let tracer = provider
            .tracer_builder(invocation_target.service_name().to_string())
            .build();

        tracing::debug!("# invocation: {} prefix: {:?}", invocation_target, prefix);
        let attributes = vec![
            KeyValue::new(RPC_SERVICE, invocation_target.service_name().to_string()),
            KeyValue::new(RPC_METHOD, invocation_target.handler_name().to_string()),
            KeyValue::new(RPC_SYSTEM, "restate"),
            KeyValue::new(INVOCATION_ID, invocation_id.to_string()),
            KeyValue::new(INVOCATION_TARGET, invocation_target.to_string()),
        ];

        let name = match prefix {
            None => invocation_target.handler_name().to_string(),
            Some(prefix) => format!("{} {}", prefix, invocation_target.handler_name()),
        };

        let inner = tracer.span_builder(name).with_kind(SpanKind::Server);

        InvocationSpanBuilder {
            parent: parent_ctx,
            inner,
            attributes,
            tracer,

            start_time: None,
            trace_id: None,
            span_id: None,
        }
    }

    pub fn with_start_time(mut self, start_time: impl Into<SystemTime>) -> Self {
        self.start_time = Some(start_time.into());
        self
    }

    pub fn with_span_id(mut self, span_id: impl Into<SpanId>) -> Self {
        self.span_id = Some(span_id.into());
        self
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<TraceId>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    pub fn start(mut self, attributes: impl IntoIterator<Item = KeyValue>) -> BoxedSpan {
        self.attributes.extend(attributes);

        let mut builder = self.inner.with_attributes(self.attributes);
        if let Some(start_time) = self.start_time {
            builder = builder.with_start_time(start_time);
        }
        if let Some(trace_id) = self.trace_id {
            builder = builder.with_trace_id(trace_id);
        }
        if let Some(span_id) = self.span_id {
            builder = builder.with_span_id(span_id);
        }

        //builder.with_span_id(span_id)
        // builder.with_start_time(start_time)
        let span = match self.parent {
            Some(ctx) => {
                tracing::debug!("# parent-ctx: {:?}", ctx);
                builder.start_with_context(&self.tracer, ctx)
            }
            None => builder.start(&self.tracer),
        };

        span
    }
}

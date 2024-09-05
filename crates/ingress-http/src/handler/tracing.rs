// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ConnectInfo;

use http::Request;
use opentelemetry::global::ObjectSafeSpan;
use opentelemetry::trace::SpanContext;
use restate_tracing_instrumentation as instrumentation;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationTarget;

pub(crate) fn prepare_tracing_span<B>(
    invocation_id: &InvocationId,
    invocation_target: &InvocationTarget,
    req: &Request<B>,
) -> SpanContext {
    let connect_info: &ConnectInfo = req
        .extensions()
        .get()
        .expect("Should have been injected by the previous layer");
    let (client_addr, client_port) = (connect_info.address(), connect_info.port());

    let span = instrumentation::info_invocation_span!(
        relation = SpanRelation::None,
        prefix = "ingress",
        id = invocation_id,
        target = invocation_target,
        tags = (
            client.socket.address = client_addr.to_string(),
            client.socket.port = client_port as i64
        )
    );

    span.span_context().clone()
}

use restate_types::{identifiers::InvocationId, invocation::InvocationTarget};
use restate_wal_protocol::Command;
use tracing::{debug_span, event_enabled, trace_span, Level, Span};

pub(super) trait SpanExt {
    fn record_invocation_id(&self, id: &InvocationId);
    fn record_invocation_target(&self, target: &InvocationTarget);
}

impl SpanExt for tracing::Span {
    fn record_invocation_id(&self, id: &InvocationId) {
        self.record("restate.invocation.id", tracing::field::display(id));
    }

    fn record_invocation_target(&self, target: &InvocationTarget) {
        self.record("restate.invocation.target", tracing::field::display(target));
        self.record(
            "rpc.service",
            tracing::field::display(target.service_name()),
        );
        self.record("rpc.method", tracing::field::display(target.handler_name()));
    }
}

pub(super) fn state_machine_apply_command_span(is_leader: bool, cmd: &Command) -> Span {
    let span = if is_leader {
        debug_span!(
            "apply_command",
            otel.name = format!("apply-command: {}", cmd.name()),
            restate.invocation.id = tracing::field::Empty,
            restate.invocation.target = tracing::field::Empty,
            rpc.service = tracing::field::Empty,
            rpc.method = tracing::field::Empty,
            restate.state_machine.command = tracing::field::debug(cmd),
        )
    } else {
        trace_span!(
            "apply_command",
            otel.name = format!("apply-command: {}", cmd.name()),
            restate.invocation.id = tracing::field::Empty,
            restate.invocation.target = tracing::field::Empty,
            rpc.service = tracing::field::Empty,
            rpc.method = tracing::field::Empty,
            restate.state_machine.command = tracing::field::debug(cmd),
        )
    };
    if event_enabled!(Level::TRACE) {
        span.record("restate.state_machine.command", tracing::field::debug(cmd));
    }

    span
}

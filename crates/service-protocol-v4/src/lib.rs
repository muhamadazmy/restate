// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains the code-generated structs of [service-protocol](https://github.com/restatedev/service-protocol) and the codec to use them.

#[cfg(feature = "entry-codec")]
pub mod entry_codec;
#[cfg(feature = "message-codec")]
pub mod message_codec;

#[allow(clippy::enum_variant_names)]
// We need to allow dead code because the entry-codec feature only uses a subset of the defined
// service protocol messages. Otherwise, crates depending only on this feature fail clippy.
#[allow(dead_code)]
pub mod proto {

    include!(concat!(env!("OUT_DIR"), "/dev.restate.service.protocol.rs"));

    #[cfg(feature = "message-codec")]
    crate::message_codec::default_encode_decode!(
        StartMessage,
        ErrorMessage,
        EndMessage,
        CommandAckMessage,
        ProposeRunCompletionMessage,
        CallCommandMessage,
        OneWayCallCommandMessage
    );
}

// include definitions of olf and obsolete messages that
// are no long in use but kept for backward compatibility
#[allow(dead_code)]
mod legacy {
    include!(concat!(env!("OUT_DIR"), "/dev.restate.service.legacy.rs"));
}

mod dto {
    use prost::Message;
    use restate_types::service_protocol::ServiceProtocolVersion;

    use crate::{
        message_codec::{ServiceWireDecoder, ServiceWireEncoder},
        proto::{CombinatorType, Future, SuspensionMessage},
    };

    impl SuspensionMessage {
        fn flatten_future(
            fut: &Future,
            completions: &mut Vec<u32>,
            signals: &mut Vec<u32>,
            named_signals: &mut Vec<String>,
        ) {
            completions.extend_from_slice(&fut.waiting_completions);
            signals.extend_from_slice(&fut.waiting_signals);
            named_signals.extend_from_slice(&fut.waiting_named_signals);

            for sub in &fut.nested_futures {
                Self::flatten_future(sub, completions, signals, named_signals);
            }
        }

        /// Flattens all completions, signals and named signals
        /// for compatibility with suspension message v6
        pub fn flatten(&self) -> (Vec<u32>, Vec<u32>, Vec<String>) {
            let mut completions = vec![];
            let mut signals = vec![];
            let mut named_signals = vec![];

            if let Some(fut) = &self.awaiting_on {
                Self::flatten_future(fut, &mut completions, &mut signals, &mut named_signals);
            }

            (completions, signals, named_signals)
        }

        fn as_suspension_v6(&self) -> SuspensionMessageV6 {
            let (waiting_completions, waiting_signals, waiting_named_signals) = self.flatten();

            SuspensionMessageV6 {
                waiting_completions,
                waiting_signals,
                waiting_named_signals,
            }
        }
    }

    use crate::legacy::SuspensionMessageV6;

    impl From<SuspensionMessageV6> for SuspensionMessage {
        fn from(value: SuspensionMessageV6) -> Self {
            let fut = Future {
                combinator_type: CombinatorType::FirstCompleted.into(),
                waiting_completions: value.waiting_completions,
                waiting_signals: value.waiting_signals,
                waiting_named_signals: value.waiting_named_signals,
                ..Default::default()
            };

            Self {
                awaiting_on: Some(fut),
            }
        }
    }

    /// The ServiceWireEncoder is only for completion since
    /// it's required by the wire Message types. In reality
    /// there is no scenario where we actually do SuspensionMessage::encode
    impl ServiceWireEncoder for SuspensionMessage {
        fn encode(
            &self,
            buf: &mut impl bytes::BufMut,
            service_protocol_version: ServiceProtocolVersion,
        ) -> Result<(), crate::message_codec::MessageEncodingError> {
            assert_ne!(
                service_protocol_version,
                ServiceProtocolVersion::Unspecified
            );

            match service_protocol_version {
                ServiceProtocolVersion::Unspecified => {
                    unreachable!("unspecified service protocol version should never be selected");
                }
                ServiceProtocolVersion::V1
                | ServiceProtocolVersion::V2
                | ServiceProtocolVersion::V3
                | ServiceProtocolVersion::V4
                | ServiceProtocolVersion::V5
                | ServiceProtocolVersion::V6 => {
                    let suspension_v6 = self.as_suspension_v6();
                    prost::Message::encode(&suspension_v6, buf)?;
                }
                ServiceProtocolVersion::V7 => prost::Message::encode(self, buf)?,
            };

            Ok(())
        }

        fn encoded_len(
            &self,
            service_protocol_version: restate_types::service_protocol::ServiceProtocolVersion,
        ) -> usize {
            assert_ne!(
                service_protocol_version,
                ServiceProtocolVersion::Unspecified
            );

            match service_protocol_version {
                ServiceProtocolVersion::Unspecified => {
                    unreachable!("unspecified service protocol version should never be selected");
                }
                ServiceProtocolVersion::V1
                | ServiceProtocolVersion::V2
                | ServiceProtocolVersion::V3
                | ServiceProtocolVersion::V4
                | ServiceProtocolVersion::V5
                | ServiceProtocolVersion::V6 => {
                    let suspension_v6 = self.as_suspension_v6();
                    prost::Message::encoded_len(&suspension_v6)
                }
                ServiceProtocolVersion::V7 => prost::Message::encoded_len(self),
            }
        }
    }

    impl ServiceWireDecoder for SuspensionMessage {
        fn decode(
            buf: impl bytes::Buf,
            service_protocol_version: restate_types::service_protocol::ServiceProtocolVersion,
        ) -> Result<Self, crate::message_codec::MessageEncodingError> {
            assert_ne!(
                service_protocol_version,
                ServiceProtocolVersion::Unspecified
            );

            let msg = match service_protocol_version {
                ServiceProtocolVersion::Unspecified => {
                    unreachable!("unspecified service protocol version should never be selected");
                }
                ServiceProtocolVersion::V1
                | ServiceProtocolVersion::V2
                | ServiceProtocolVersion::V3
                | ServiceProtocolVersion::V4
                | ServiceProtocolVersion::V5
                | ServiceProtocolVersion::V6 => SuspensionMessageV6::decode(buf)?.into(),
                ServiceProtocolVersion::V7 => <SuspensionMessage as prost::Message>::decode(buf)?,
            };

            Ok(msg)
        }
    }
}

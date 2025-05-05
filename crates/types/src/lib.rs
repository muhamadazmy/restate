// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains the core types used by various Restate components.

mod base62_util;
mod id_util;
mod macros;
mod node_id;
mod version;

pub mod art;
pub mod cluster;
pub mod health;

pub mod config;
pub mod config_loader;
pub mod deployment;
pub mod endpoint_manifest;
pub mod epoch;
pub mod errors;
pub mod identifiers;
pub mod invocation;
pub mod journal;
pub mod journal_v2;
pub mod live;
pub mod locality;
pub mod logs;
pub mod message;
pub mod metadata;
pub mod metadata_store;
pub mod net;
pub mod nodes_config;
pub mod partition_table;
pub mod protobuf;
pub mod replicated_loglet;
pub mod replication;
pub mod retries;
pub mod schema;
pub mod service_discovery;
pub mod service_protocol;
pub mod state_mut;
pub mod storage;
pub mod time;
pub mod timer;

use std::ops::RangeInclusive;

use bilrost::encoding::{EmptyState, General, ValueDecoder, ValueEncoder};

use restate_encoding::{BilrostAs, NetSerde};

pub use id_util::{IdDecoder, IdEncoder, IdResourceType, IdStrCursor};
pub use node_id::*;
pub use version::*;

// Re-export metrics' SharedString (Space-efficient Cow + RefCounted variant)
pub type SharedString = metrics::SharedString;

/// Trait for merging two attributes
pub trait Merge {
    /// Return true if the value was mutated as a result of the merge
    fn merge(&mut self, other: Self) -> bool;
}

impl Merge for bool {
    fn merge(&mut self, other: Self) -> bool {
        if *self != other {
            *self |= other;
            true
        } else {
            false
        }
    }
}

/// A wrapper around [`enumset::EnumSet`] type that can serialize
/// as a bilrost message
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    BilrostAs,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
)]
#[bilrost_as(EnumSetWire)]
pub struct NetEnumSet<T>(enumset::EnumSet<T>)
where
    T: enumset::EnumSetType + 'static;

impl<T> NetSerde for NetEnumSet<T> where T: NetSerde + enumset::EnumSetType {}

impl<T> Default for NetEnumSet<T>
where
    T: enumset::EnumSetType,
{
    fn default() -> Self {
        Self(enumset::EnumSet::empty())
    }
}

#[derive(bilrost::Message)]
struct EnumSetWire(u64);

impl<T> From<&NetEnumSet<T>> for EnumSetWire
where
    T: enumset::EnumSetType,
{
    fn from(value: &NetEnumSet<T>) -> Self {
        Self(value.0.as_u64())
    }
}

impl<T> From<EnumSetWire> for NetEnumSet<T>
where
    T: enumset::EnumSetType + 'static,
{
    fn from(value: EnumSetWire) -> Self {
        Self(enumset::EnumSet::<T>::from_u64_truncated(value.0))
    }
}

impl<T> From<T> for NetEnumSet<T>
where
    T: enumset::EnumSetType,
{
    fn from(value: T) -> Self {
        enumset::EnumSet::from(value).into()
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BilrostAs,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
)]
#[bilrost_as(RangeInclusiveWire<Idx>)]
pub struct NetRangeInclusive<Idx>(RangeInclusive<Idx>)
where
    Idx: Copy + EmptyState + ValueEncoder<General> + ValueDecoder<General> + 'static;

impl<Idx> Default for NetRangeInclusive<Idx>
where
    Idx: Copy + EmptyState + ValueEncoder<General> + ValueDecoder<General>,
{
    fn default() -> Self {
        Self(RangeInclusive::new(Idx::empty(), Idx::empty()))
    }
}

#[derive(bilrost::Message)]
struct RangeInclusiveWire<Idx>((Idx, Idx))
where
    Idx: EmptyState + ValueEncoder<General> + ValueDecoder<General>;

impl<Idx> From<&NetRangeInclusive<Idx>> for RangeInclusiveWire<Idx>
where
    Idx: Copy + EmptyState + ValueEncoder<General> + ValueDecoder<General>,
{
    fn from(value: &NetRangeInclusive<Idx>) -> Self {
        Self((*value.0.start(), *value.0.end()))
    }
}

impl<Idx> From<RangeInclusiveWire<Idx>> for NetRangeInclusive<Idx>
where
    Idx: Copy + EmptyState + ValueEncoder<General> + ValueDecoder<General>,
{
    fn from(value: RangeInclusiveWire<Idx>) -> Self {
        Self(RangeInclusive::new(value.0.0, value.0.1))
    }
}

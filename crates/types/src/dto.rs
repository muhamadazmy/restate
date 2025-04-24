// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;

/// A trait for converting between internal types and their DTO (Data Transfer Object)
/// representations used for storage or wire transmission.
///
/// Types that need to be serialized or sent over the network should implement this trait,
/// defining how they convert to and from a `Target` type, which is expected to implement
/// both [`bilrost::Message`] and [`bilrost::OwnedMessage`].
///
/// For types that already implement [`bilrost::Message`], a blanket implementation is provided,
/// allowing them to be stored or transferred directly without transformation.
pub trait DataTransferObject: Sized {
    type Target: bilrost::Message + bilrost::OwnedMessage;
    type Error: std::error::Error + Send + Sync + 'static;

    fn into_dto(self) -> Self::Target;
    fn from_dto(value: Self::Target) -> Result<Self, Self::Error>;
}

impl<T> DataTransferObject for T
where
    T: bilrost::Message + bilrost::OwnedMessage,
{
    type Target = T;
    type Error = Infallible;

    fn from_dto(value: Self::Target) -> Result<Self, Self::Error> {
        Ok(value)
    }

    fn into_dto(self) -> Self::Target {
        self
    }
}

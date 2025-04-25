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

use restate_types_derive::BilrostNewType;

/// A trait for converting from internal types and their DTO (Data Transfer Object)
/// representations used for storage or wire transmission.
///
/// Types that need to be serialized or sent over the network should implement this trait,
/// defining how they convert to and from a `Target` type, which is expected to implement
/// [`bilrost::Message`]
///
/// For types that already implement [`bilrost::Message`], a blanket implementation is provided,
/// allowing them to be stored or transferred directly without transformation.
pub trait IntoDto {
    type Target: bilrost::Message;
    fn into_dto(self) -> Self::Target;
}

/// A trait for converting from DTOs (Data Transfer Object) used mainly for storage or wire transmission, and
/// their internal types used for in memory manipulation
///
/// Types that need to be serialized or sent over the network should implement this trait,
/// defining how they convert to and from a `Target` type, which is expected to implement
/// [`bilrost::OwnedMessage`]
///
/// For types that already implement [`bilrost::Message`], a blanket implementation is provided,
/// allowing them to be stored or transferred directly without transformation.
///
pub trait FromDto: Sized {
    type Target: bilrost::OwnedMessage;
    type Error: std::error::Error + Send + Sync + 'static;

    fn from_dto(value: Self::Target) -> Result<Self, Self::Error>;
}

impl<T> IntoDto for T
where
    T: bilrost::Message,
{
    type Target = T;

    fn into_dto(self) -> Self::Target {
        self
    }
}

impl<T> FromDto for T
where
    T: bilrost::OwnedMessage,
{
    type Target = T;
    type Error = Infallible;

    fn from_dto(value: Self::Target) -> Result<Self, Self::Error> {
        Ok(value)
    }
}

/// A common u128 dto type
#[derive(Debug, Clone, Copy, PartialEq, Eq, BilrostNewType)]
struct U128((u64, u64));

impl From<u128> for U128 {
    fn from(value: u128) -> Self {
        Self(((value >> 64) as u64, value as u64))
    }
}

impl From<U128> for u128 {
    fn from(value: U128) -> Self {
        (value.0.0 as u128) << 64 | value.0.1 as u128
    }
}

#[cfg(test)]
mod test {
    use rand::random;

    use super::U128;

    #[test]
    fn test_u128() {
        (0..100).for_each(|_| {
            let num = random::<u128>();
            let value = U128::from(num);

            assert_eq!(num, u128::from(value));
        });
    }
}

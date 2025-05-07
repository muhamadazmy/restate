// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZero;

use bilrost::encoding::{EmptyState, ForOverwrite, General, ValueDecoder, ValueEncoder, Wiretyped};
use restate_encoding_derive::BilrostNewType;

use crate::NetSerde;

/// A Bilrost compatible U128 type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, BilrostNewType)]
pub struct U128((u64, u64));

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

impl NetSerde for U128 {}

/// A bilrost compatible wrapper on top of Option<NonZero<T>> that encodes
/// as `zero` if is set to None.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct OptionNonZero<P>(Option<P>);

impl<P> Default for OptionNonZero<P> {
    fn default() -> Self {
        Self(None)
    }
}

impl<Primitive> NetSerde for OptionNonZero<Primitive> where Primitive: NetSerde {}

macro_rules! impl_option_non_zero {
    ($t:ty) => {
        impl From<OptionNonZero<$t>> for Option<NonZero<$t>> {
            fn from(value: OptionNonZero<$t>) -> Self {
                value
                    .0
                    .map(|v| NonZero::<$t>::try_from(v).expect("is non-zero"))
            }
        }

        impl From<Option<NonZero<$t>>> for OptionNonZero<$t> {
            fn from(value: Option<NonZero<$t>>) -> Self {
                Self(value.map(|v| v.get()))
            }
        }
    };
    ($($t:ty),+) => {
        $(impl_option_non_zero!($t);)+
    }
}

impl_option_non_zero!(u8, u16, u32, u64, usize);

impl<P> ValueEncoder<General> for OptionNonZero<P>
where
    P: ValueEncoder<General> + Default + Copy,
{
    fn encode_value<B: ::bytes::BufMut + ?Sized>(value: &Self, buf: &mut B) {
        let value = value.0.unwrap_or_default();
        <P as ValueEncoder<General>>::encode_value(&value, buf)
    }

    fn value_encoded_len(value: &Self) -> usize {
        let value = value.0.unwrap_or_default();
        <P as ValueEncoder<General>>::value_encoded_len(&value)
    }

    fn prepend_value<B: bilrost::buf::ReverseBuf + ?Sized>(value: &Self, buf: &mut B) {
        let value = value.0.unwrap_or_default();
        <P as ValueEncoder<General>>::prepend_value(&value, buf)
    }
}

#[allow(clippy::all)]
impl<P> ValueDecoder<General> for OptionNonZero<P>
where
    P: ValueDecoder<General> + Default + Copy + PartialEq,
{
    fn decode_value<B: ::bytes::Buf + ?Sized>(
        value: &mut Self,
        buf: ::bilrost::encoding::Capped<B>,
        ctx: ::bilrost::encoding::DecodeContext,
    ) -> ::std::result::Result<(), ::bilrost::DecodeError> {
        let zero = P::default();
        let mut inner = zero;
        <P as ValueDecoder<General>>::decode_value(&mut inner, buf, ctx)?;
        if inner != zero {
            value.0 = Some(inner);
        }

        Ok(())
    }
}

#[allow(clippy::all)]
impl<P> Wiretyped<General> for OptionNonZero<P>
where
    P: Wiretyped<General>,
{
    const WIRE_TYPE: ::bilrost::encoding::WireType = <P as Wiretyped<General>>::WIRE_TYPE;
}

#[allow(clippy::all)]
impl<P> EmptyState for OptionNonZero<P> {
    fn clear(&mut self) {
        self.0 = None;
    }
    fn empty() -> Self
    where
        Self: Sized,
    {
        Self::default()
    }
    fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

impl<P> ForOverwrite for OptionNonZero<P> {
    fn for_overwrite() -> Self
    where
        Self: Sized,
    {
        Self::default()
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

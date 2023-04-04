//! This crate contains all the relevant error codes used by the Restate Runtime.
//!
//! To add a new error code:
//! * Add the code name to the macro invocation below.
//! * Add a new markdown file under `error_codes/` with the same name of the error code. This file should contain the description of the error.
//! * Now you can use the code declaration in conjunction with the [`codederror::CodedError`] macro as follows:
//!
//! ```rust,ignore
//! use codederror::CodedError;
//! use thiserror::Error;
//!
//! #[derive(Error, CodedError, Debug)]
//! #[code(RT0001)]
//! #[error("my error")]
//! pub struct MyError;
//! ```
//!

#[macro_use]
pub mod fmt;
#[macro_use]
mod helper;

// RT are runtime related errors and can be used for both execution errors, or runtime configuration errors.
// META are meta related errors.

declare_restate_error_codes!(RT0001, META0001, META0002, META0003);
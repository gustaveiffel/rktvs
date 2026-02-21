// SPDX-License-Identifier: AGPL-3.0-only

//! QUIC transport layer for rk hub-satellite communication.
//!
//! Provides a hub server and satellite client over QUIC (quinn/rustls),
//! self-signed TLS certificate management, protobuf wire protocol,
//! and catalog sync.

pub mod cert;
pub mod frame;
pub mod hub;
pub mod proto;
pub mod satellite;

#[cfg(feature = "test-support")]
pub mod link_simulator;

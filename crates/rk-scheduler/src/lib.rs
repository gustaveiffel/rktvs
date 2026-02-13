// SPDX-License-Identifier: AGPL-3.0-only

//! Cost-aware scheduling, estimation, and resumable chunk fetching for rk.
//!
//! Implements the Grade x CostTier decision matrix, transfer cost estimation
//! without data transfer, and chunk-level resumable fetching with job tracking.

pub mod estimate;
pub mod fetcher;
pub mod types;

#![forbid(unsafe_code)]

pub mod error;
pub mod wheel;

pub use error::WheelError;
pub use wheel::{DEFAULT_TICK_MS, DEFAULT_WHEEL_SIZE, TimingWheel};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WheelError {
    #[error("execute_at {execute_at} is before the wheel start time {start_ms}")]
    TimeBeforeStart { execute_at: i64, start_ms: i64 },

    /// `execute_at` is so far out that the tick computation would overflow.
    /// With a preloader in front of the wheel this should not happen in
    /// practice, but the bound has to be enforced somewhere.
    #[error("execute_at {execute_at} exceeds the wheel horizon of {max_horizon_ms} ms")]
    DeadlineTooFar {
        execute_at: i64,
        max_horizon_ms: u64,
    },
}

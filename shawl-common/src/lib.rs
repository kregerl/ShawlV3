use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ScanResults {
    // The range that was scanned
    pub id: String,
    pub servers: Vec<craftping::Response>,
}

pub const HEARTBEAT_EXCHANGE: &'static str = "heartbeat";
pub const IP_RANGE_EXCHANGE: &'static str = "ranges";
pub const RESULTS_EXCHANGE: &'static str = "results";
pub const AUTOMATIC_QUEUE_NAME: &'static str = "";
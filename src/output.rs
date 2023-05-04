use thinp::report::*;
use std::sync::Arc;


// A structure to encapsulate related output options and output formattting.
pub struct Output {
    pub report: Arc<Report>,
    pub json: bool,
}
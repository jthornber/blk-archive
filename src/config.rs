use serde_derive::{Deserialize, Serialize};

//-----------------------------------------

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub block_size: usize,
    pub splitter_alg: String,
}

//-----------------------------------------


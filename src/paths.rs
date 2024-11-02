use std::path::PathBuf;

//------------------------------

pub fn index_path() -> PathBuf {
    ["indexes", "seen"].iter().collect()
}

pub fn data_path() -> PathBuf {
    ["data", "data"].iter().collect()
}

pub fn hashes_path() -> PathBuf {
    ["data", "hashes"].iter().collect()
}

pub fn stream_path(stream: &str) -> PathBuf {
    ["streams", stream, "stream"].iter().collect()
}

pub fn stream_path_offsets(stream: &str) -> PathBuf {
    ["streams", stream, "stream.offsets"].iter().collect()
}

pub fn stream_config(stream: &str) -> PathBuf {
    ["streams", stream, "config.toml"].iter().collect()
}

//------------------------------

use std::path::PathBuf;

//------------------------------

pub fn index_path() -> PathBuf {
    ["indexes", "seen"].iter().collect()
}

//------------------------------

#[allow(unused_variables)]
#[allow(dead_code)]
use crate::catalogue::RootCatalogue;
use crate::utils::{ csv_tools::reader::BlobWriter, storage::storage::BackEnd };

pub struct EngineOptions {
    // Object Store Client
}

impl EngineOptions {
    pub fn provider(mut self) -> Self {
        unimplemented!()
    }

    pub fn build(mut self) -> anyhow::Result<LakeEngine> {
        // Instantiate object store client

        unimplemented!()
    }
}

pub struct LakeEngine {
    blob_writer: BlobWriter,
    catalogue: RootCatalogue,
    engine_state: BackEnd,
}

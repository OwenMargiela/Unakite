#[allow(unused_variables)]
#[allow(dead_code)]
use std::path::PathBuf;

use datafusion::prelude::SessionContext;

use crate::{ blob_writer::{ BackEnd, CloudClient }, catalogue::RootCatalogue };
use crate::utils::csv_tools::reader::BlobWriter;

pub struct EngineOptions {
    local: bool,
    cloud_client: Option<CloudCredentials>,
}

pub struct CloudCredentials {
    endpoint: PathBuf,
    container_name: String,
    blob_name: String,
}

impl EngineOptions {
    pub fn local(mut self, bool: bool) -> Self {
        self.local = bool;
        self
    }

    pub fn cloud_provider(mut self, cloud_cred: CloudCredentials) -> Self {
        self.cloud_client = Some(cloud_cred);

        self
    }

    pub fn build(mut self) -> anyhow::Result<LakeEngine> {

        // Create a datafusion listing table the references either the cloud object store of the local storage directories
        if let Some(cloud_provider) = self.cloud_client {
            // Instantiate Cloud Client with cloud credentials
            let storage_backend = BackEnd::Cloud(CloudClient {});
        }
        unimplemented!()
    }
}

pub struct LakeEngine {
    blob_writer: BlobWriter,
    catalogue: RootCatalogue,
    sql_engine: SessionContext,
}

use std::path::PathBuf;
#[allow(unused_variables)]
#[allow(dead_code)]
use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::{ blob_writer::{ BackEnd, CloudClient, StorageLoader }, catalogue::RootCatalogue };

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
        if let Some(cloud_provider) = self.cloud_client {
            // Instantiate Cloud Client with cloud credentials
            let storage_backend = BackEnd(CloudClient);
        }
        unimplemented!()
    }
}

pub struct LakeEngine {
    engine: Arc<SessionContext>,
    blob_writer: StorageLoader,
    catalogue: RootCatalogue,
}

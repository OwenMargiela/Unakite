use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::{ blob_writer::StorageLoader, catalogue::{ RootCatalogue, RootCatalogueBuilder } };

pub struct EngineOptions {
    local: bool,
    cloud_client: CloudCredentials,
}

pub struct CloudCredentials; // TODO

impl EngineOptions {
    pub fn local(mut self, bool: bool) -> Self {
        self.local = bool;
        self
    }

    pub fn cloud_provider(mut self, cloud_cred: CloudCredentials) -> Self {
        self.cloud_client = cloud_cred;
        self
    }

    pub fn build() -> anyhow::Result<LakeEngine> {
        unimplemented!()
    }
}

pub(crate) struct LakeEngine {
    engine: Arc<SessionContext>,
    blob_writer: StorageLoader,
    catalogue: RootCatalogue,
}

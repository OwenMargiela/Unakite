use std::{ path::PathBuf, sync::Arc };

use anyhow::Ok;
use object_store::{ local::LocalFileSystem, ObjectStore };

use crate::utils::csv_tools::file_utils::LOCAL_DB_ROOT;

pub struct BackEnd {
    store: Arc<dyn ObjectStore>,
    store_meta: Storage,
}

pub enum Storage {
    LocalFileSystem {
        base_path: PathBuf,
    },

    S3 {
        bucket: String,
        region: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        endpoint: Option<String>, // For MinIO or custom endpoints
        prefix: Option<String>,
    },
}

impl Storage {
    pub async fn get_store(self) -> anyhow::Result<BackEnd> {
        match self {
            Self::LocalFileSystem { ref base_path } => {
                let store: Arc<dyn ObjectStore> = Arc::new(
                    LocalFileSystem::new_with_prefix(LOCAL_DB_ROOT)?
                );

                return Ok(BackEnd {
                    store,
                    store_meta: self,
                });
            }

            _ => {
                // TODO
                unimplemented!();
            }
        }
    }
}

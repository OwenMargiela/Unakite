use std::{ path::PathBuf, sync::Arc };

use arrow_schema::Schema;
use datafusion::prelude::SessionContext;

/// Represents input data
pub enum DataFile {
    Parquet(PathBuf),
    Csv(PathBuf),
}

/// Represents where the data will be stored
pub enum BackEnd {
    Local(PathBuf),
    Cloud(CloudClient),
}

pub struct CloudClient; // TODO: define cloud upload logic

/// Storage abstraction
pub struct StorageLoader {
    source: PathBuf,
    schema: Arc<Schema>,
    backend: BackEnd,
}

impl StorageLoader {
    pub fn new(
        data_file: DataFile,
        schema: Schema,

        backend: BackEnd
    ) -> Self {
        let source = match data_file {
            DataFile::Parquet(path) => path,
            DataFile::Csv(path) => {
                // TODO: CSV → Parquet conversion, return path to parquet
                // For now, we use the original path
                path
            }
        };

        Self {
            source,
            schema: Arc::new(schema),

            backend,
        }
    }

    /// Partition and store data locally
    pub fn store_local(
        &self,
        partitions: &[String],
        engine: Arc<SessionContext>
    ) -> anyhow::Result<()> {
        self.process_and_store(partitions, engine)
    }

    /// Partition and send data to cloud
    pub fn send_to_cloud(
        &self,
        partitions: &[String],
        engine: Arc<SessionContext>
    ) -> anyhow::Result<()> {
        self.process_and_store(partitions, engine)
    }

    fn process_and_store(
        &self,
        partitions: &[String],
        engine: Arc<SessionContext>
    ) -> anyhow::Result<()> {
        // 1. Register source file
        // 2. Run partitioned query using `engine`
        // 3. Write output partitions to either local or cloud if needed
        // 4. Update internal catalog (if needed)

        if !partitions.is_empty() {
            // Partition logic here
        }

        match &self.backend {
            BackEnd::Local(dir) => {
                // Save partitions to local filesystem
                println!("Saving to local path: {:?}", dir);
            }
            BackEnd::Cloud(client) => {
                // Upload partitions via cloud client
                println!("Uploading to cloud");
            }
        }

        Ok(())
    }
}

use std::{ path::PathBuf, sync::Arc };

use anyhow::Ok;
use arrow_schema::Schema;
use datafusion::prelude::SessionContext;

/// Represents input data file
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

    engine_ref: Arc<SessionContext>,
}

impl StorageLoader {
    pub fn new(
        data_file: DataFile,
        schema: Schema,

        backend: BackEnd,
        engine: Arc<SessionContext>
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
            engine_ref: engine,

            backend,
        }
    }

    // Should really be an async function

    fn store(&self, partitions: &[String]) -> anyhow::Result<()> {
        // 1. Register source file
        // 2. Run partitioned query using `engine`
        // 3. Write output partitions to either local or cloud if needed
        // 4. Update internal catalog (if needed)

        if !partitions.is_empty() {
            // Partition logic here
            self.process_partition()?;
            return Ok(());
        }

        match &self.backend {
            BackEnd::Local(dir) => {
                // Save partitions to local filesystem
                println!("Saving to local path: {:?}", dir);
                self.process_partition()?;
            }
            BackEnd::Cloud(client) => {
                // Upload partitions via cloud client
                println!("Uploading to cloud");
                self.process_partition()?;
            }
        }

        Ok(())
    }

    fn process_partition(&self) -> anyhow::Result<()> {
        unimplemented!()
    }
}

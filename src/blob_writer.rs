use std::{ path::PathBuf, sync::Arc };

use anyhow::Ok;
use arrow_schema::Schema;

use crate::utils::csv_tools::{ file_utils::LOCAL_DB_ROOT, reader::BlobWriter };

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

impl BlobWriter {
    async fn store(&self, partitions: Vec<String>) -> anyhow::Result<()> {
        // 1. Register source file
        // 2. Run partitioned query using `engine`
        // 3. Write output partitions to either local or cloud if needed
        // 4. Update internal catalog (if needed)

        match &self.back_end {
            BackEnd::Local(dir) => {
                // Save partitions to local filesystem
                println!("Saving to local path: {:?}", dir);

                if !partitions.is_empty() {
                    // Partition logic here
                    self.partion_on(partitions, LOCAL_DB_ROOT.to_string()).await?;
                    return Ok(());
                }

                self.write_parquet()?;
            }
            BackEnd::Cloud(client) => {
                // Upload partitions via cloud client's chunking strategy

                println!("Uploading to cloud");
            }
        }

        Ok(())
    }
}

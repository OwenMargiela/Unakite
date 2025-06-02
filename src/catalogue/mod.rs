#![allow(unused_variables)]

#[allow(dead_code)]
pub mod tables;
pub mod sql_strings;

use std::path::PathBuf;
use dashmap::DashMap;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::catalogue::sql_strings::CREATE_SYSTEM_TABLE_SQL;

///
///
/// Tasks like creating, dropping, and renaming tables are the responsibility of a catalog.
///
///
/// The most important responsibility of a catalog is tracking a table's current metadata,
///
/// which is provided by the catalog when you load a table from local or cloud stores in the
/// form of parquet files.

pub(crate) struct RootCatalogue {
    db: Pool<SqliteConnectionManager>,
    root_dir: RootStoragePath,
    tables: DashMap<String, u32>,
}

pub(crate) enum RootStoragePath {
    Local(PathBuf),
    Cloud(PathBuf),
}

pub(crate) struct RootCatalogueBuilder {
    db_path: PathBuf,
    root_dir: RootStoragePath,
}

impl RootCatalogueBuilder {
    pub(crate) fn new() -> Self {
        Self {
            db_path: PathBuf::from("db.db"),
            root_dir: RootStoragePath::Local("db/".into()),
        }
    }

    pub(crate) fn with_cloud_provider(mut self, path: impl Into<PathBuf>) -> Self {
        self.root_dir = RootStoragePath::Cloud(path.into());
        self
    }

    pub(crate) fn build(self) -> RootCatalogue {
        let manager = SqliteConnectionManager::file(&self.db_path);
        let pool = Pool::builder().build(manager).expect("DB pool failed");

        let conn = pool.get().unwrap();
        conn.execute_batch(CREATE_SYSTEM_TABLE_SQL).expect("System table creation failed");

        RootCatalogue {
            db: pool,
            root_dir: self.root_dir,
            tables: DashMap::new(),
        }
    }
}



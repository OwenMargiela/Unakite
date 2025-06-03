#[allow(unused_variables)]
#[allow(dead_code)]
pub mod tables;
pub mod sql_strings;
pub mod catalogue_storage;

use std::{ fs::remove_file, path::PathBuf };
use dashmap::DashMap;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use anyhow;

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
    pub(crate) db: Pool<SqliteConnectionManager>,
    pub(crate) tables: DashMap<i64, String>,
}

impl RootCatalogue {
    pub(crate) fn start() -> anyhow::Result<Self> {
        let db_path = PathBuf::from("db.db");

        let manager = SqliteConnectionManager::file(&db_path);

        let pool = Pool::builder().build(manager).expect("DB pool failed");

        let conn = pool.get().unwrap();
        conn.execute_batch(CREATE_SYSTEM_TABLE_SQL).expect("System table creation failed");

        let mut statement = conn.prepare("SELECT table_id, table_name FROM sys_tables")?;

        let table_iter = statement.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;

        let tables: DashMap<i64, String> = DashMap::new();
        for table_result in table_iter {
            let (table_id, table_name) = table_result?;
            tables.insert(table_id, table_name);
        }

        Ok(RootCatalogue {
            db: pool,
            tables,
        })
    }

    pub(crate) fn destroy(self) -> anyhow::Result<()> {
        remove_file(PathBuf::from("db.db"))?;

        Ok(())
    }
}

use anyhow::Ok;
use rusqlite::params;

use crate::catalogue::{
    sql_strings::{ INSERT_SYS_TABLES, INSERT_SYS_SCHEMAS, DELETE_SYS_TABLES, DELETE_SYS_SCHEMAS },
    tables::Table,
    RootCatalogue,
};

pub trait Catalog {
    /// Creates a new table. Errors if it already exists.
    fn create_sys_table(&self, table: &Table) -> anyhow::Result<()>;
    /// Drops a table. Errors if it does not exist, unless if_exists is true.
    /// Returns true if the table existed and was deleted.
    fn del_sys_table(&self, table: i64) -> anyhow::Result<()>;
    /// Fetches a table schema
    ///
    fn get_table_schema(&self, table: &i64) -> anyhow::Result<()>;
    /// Returns a list of all table schemas.
    fn list_tables_schemas(&self) -> anyhow::Result<()>;
}

impl Catalog for RootCatalogue {
    fn create_sys_table(&self, table: &Table) -> anyhow::Result<()> {
        let mut conn = self.db.get()?;
        let tx = conn.transaction()?;

        tx.execute(INSERT_SYS_SCHEMAS, params![table.table_name, table.schema_bin])?;

        let schema_id = tx.last_insert_rowid();
        tx.execute(INSERT_SYS_TABLES, params![table.table_name, schema_id])?;

        tx.commit()?;
        // Insert into Memeory

        Ok(())
    }
    fn del_sys_table(&self, table_id: i64) -> anyhow::Result<()> {
        let mut conn = self.db.get()?;
        let tx = conn.transaction()?;

        tx.execute(DELETE_SYS_TABLES, params![table_id])?;

        tx.execute(DELETE_SYS_SCHEMAS, params![table_id])?;

        // Delete from Memeory
        Ok(())
    }

    fn get_table_schema(&self, table_id: &i64) -> anyhow::Result<()> {
        let conn = self.db.get()?;

        let mut statement = conn.prepare("SELECT schema_bin FROM sys_schemas WHERE table_id = ?")?;

        let row = statement.query_row([table_id], |row| row.get::<_, Vec<u8>>(0))?;

        // Serialize Schema

        unimplemented!()
    }

    fn list_tables_schemas(&self) -> anyhow::Result<()> {
        let conn = self.db.get()?;

        let mut statement = conn.prepare("SELECT schema_bin FROM sys_schemas")?;
        let mut rows = statement.query([])?;

        while let Some(row) = rows.next()? {
            // Serialize Table schema
            let bin = row.get::<_, Vec<u8>>(0)?;
            println!("--> Bin {:?}", bin);
        }

        Ok(())
    }
}

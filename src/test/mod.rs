#[cfg(test)]
mod tests {
    use crate::catalogue::{ catalogue_storage::Catalog, tables::Table, RootCatalogue };

    #[test]
    fn catalogue_run_start() {
        let catalogue = RootCatalogue::start().unwrap();
        let conn = catalogue.db.get();
        assert!(conn.is_ok());

        catalogue
            .create_sys_table(
                &(Table {
                    table_name: String::from("Table"),
                    schema_bin: vec![0u8, 1u8, 2u8, 3u8, 40u8, 50u8],
                    url: String::from("db://Table"),
                })
            )
            .unwrap();

        catalogue
            .create_sys_table(
                &(Table {
                    table_name: String::from("Table_two"),
                    schema_bin: vec![6u8, 7u8, 8u8, 9u8, 10u8, 20u8],
                    url: String::from("db://Table_two"),
                })
            )
            .unwrap();

        catalogue.list_tables_schemas().unwrap();

        let _ = catalogue.destroy();
    }
}

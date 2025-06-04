#[cfg(test)]
mod tests {
    use crate::catalogue::{
        catalogue_storage::Catalog,
        tables::{ Column, SchemaVec, Table },
        RootCatalogue,
    };

    #[test]
    fn schema_serde_works() {
        let mut schema = SchemaVec::new();

        schema.add(Column {
            datatype: arrow_schema::DataType::Int32,
            name: String::from("Test"),
            nullable: false,
            references: None,
            unique: false,
        });

        schema.add(Column {
            datatype: arrow_schema::DataType::Int32,
            name: String::from("Test"),
            nullable: true,
            references: Some(String::from("ID REF TABLE, ID REF TABLE")),
            unique: true,
        });

        let schema_bin = SchemaVec::serialize_schema(&schema);
        let schema_copy = SchemaVec::de_serialize_schema(schema_bin);

        assert_eq!(schema_copy, schema)
    }

    #[test]
    fn catalogue_run_start() {
        let catalogue = RootCatalogue::start().unwrap();
        let conn = catalogue.db.get();
        assert!(conn.is_ok());

        let mut schema_one = SchemaVec::new();
        schema_one.add(Column {
            datatype: arrow_schema::DataType::Int32,
            name: String::from("Test"),
            nullable: false,
            references: None,
            unique: false,
        });

        schema_one.add(Column {
            datatype: arrow_schema::DataType::Int16,
            name: String::from("West"),
            nullable: true,
            references: None,
            unique: false,
        });

        let mut schema_two = SchemaVec::new();

        schema_two.add(Column {
            datatype: arrow_schema::DataType::Int32,
            name: String::from("Test"),
            nullable: true,
            references: Some(String::from("ID REF TABLE, ID REF TABLE")),
            unique: true,
        });

        catalogue
            .create_sys_table(
                &(Table {
                    table_name: String::from("Table"),
                    schema_bin: SchemaVec::serialize_schema(&schema_one),

                    url: String::from("db://Table"),
                })
            )
            .unwrap();

        catalogue
            .create_sys_table(
                &(Table {
                    table_name: String::from("Table_two"),
                    schema_bin: SchemaVec::serialize_schema(&schema_two),
                    url: String::from("db://Table_two"),
                })
            )
            .unwrap();

        catalogue.list_tables_schemas().unwrap();

        let _ = catalogue.destroy();
    }
}

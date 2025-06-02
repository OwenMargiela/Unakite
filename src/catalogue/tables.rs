use arrow::datatypes::DataType;
use bincode::config::standard;

// A table column.
use serde::{ Serialize, Deserialize };

pub fn serialize_column(column: Column) -> Vec<u8> {
    let config = standard();

    let encoded: Vec<u8> = bincode::serde::encode_to_vec(&column, config).unwrap();

    encoded
}

pub fn de_serialize_column(encoded: Vec<u8>) -> Column {
    let config = standard();

    let (decoded_column, _): (Column, _) = bincode::serde
        ::decode_from_slice(&encoded, standard())
        .unwrap();

    decoded_column
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Column {
    /// Column name. Can't be empty.
    pub name: String,

    /// Column datatype.
    pub datatype: DataType,

    /// Whether the column allows null values. Not legal for primary keys.
    pub nullable: bool,

    /// Whether the column should only allow unique values (ignoring NULLs).
    /// Must be true for a primary key column. Requires index.
    pub unique: bool,

    /// If set, this column is a foreign key reference to the given table's
    /// primary key. Must be of the same type as the target primary key.
    /// Requires index.
    pub references: Option<String>,
}

pub struct Table {
    table_name: String,
    schema_bin: Vec<u8>,
    partitions: Vec<String>,
}

impl Table {
    pub(crate) fn new(
        table_name: String,
        partitions: Vec<String>,

        table_schema: Vec<Column>
    ) -> Self {
        unimplemented!()
    }
}

pub trait Catalog {
    /// Creates a new table. Errors if it already exists.
    fn create_table(&self, table: &Table) -> anyhow::Result<()>;
    /// Drops a table. Errors if it does not exist, unless if_exists is true.
    /// Returns true if the table existed and was deleted.
    fn drop_table(&self, table: &str, if_exists: bool) -> anyhow::Result<bool>;
    /// Fetches a table schema, or None if it doesn't exist.
    fn get_table(&self, table: &str) -> anyhow::Result<Option<String>>;
    /// Returns a list of all table schemas.
    fn list_tables(&self) -> anyhow::Result<Vec<String>>;

    /// Fetches a table schema, or errors if it does not exist.
    fn must_get_table(&self, table: &str) -> anyhow::Result<String> {
        self.get_table(table)?.ok_or_else(|| panic!("table {table} does not exist"))
    }
}

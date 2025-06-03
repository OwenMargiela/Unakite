use arrow_schema::DataType;
use bincode::config::standard;

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



pub (crate) struct  Schema {
    pub schema_id: i64,
    pub schema_bin: Vec<u8>,
}
pub(crate)  struct Table {
    pub table_name: String,
    pub schema_bin: Vec<u8>,
    pub url: String,
}



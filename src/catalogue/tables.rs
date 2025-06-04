use bytes::{ Buf, BufMut, Bytes, BytesMut };

use arrow_schema::DataType;

use bincode::config::standard;

use serde::{ Serialize, Deserialize };

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

impl Column {
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaVec {
    pub columns: Vec<Column>,
}

impl SchemaVec {
    pub fn new() -> Self {
        SchemaVec { columns: Vec::new() }
    }

    pub fn add(&mut self, column: Column) {
        self.columns.push(column);
    }

    pub fn serialize_schema(schema: &SchemaVec) -> Vec<u8> {
        let mut vec = BytesMut::with_capacity(64);

        for column in schema.columns.clone().into_iter() {
            let encoded = Bytes::from(Column::serialize_column(column));
            let encoded_length = encoded.len() as u32;

            vec.put_u32_le(encoded_length);
            vec.put(encoded);
        }

        vec.to_vec()
    }

    pub fn de_serialize_schema(encoded_buf: Vec<u8>) -> SchemaVec {
        let mut encoded = Bytes::from(encoded_buf);

        let mut columns: Vec<Column> = Vec::new();

        while encoded.has_remaining() {
            let column_len = encoded.get_u32_le();
            let mut data_buf = vec![0u8; column_len as usize];

            encoded.copy_to_slice(&mut data_buf);

            columns.push(Column::de_serialize_column(data_buf));
        }

        SchemaVec { columns }
    }
}

pub struct Table {
    pub table_name: String,
    pub schema_bin: Vec<u8>,
    pub url: String,
}

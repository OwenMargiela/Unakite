pub const CREATE_SYSTEM_TABLE_SQL: &str =
    r#"
CREATE TABLE IF NOT EXISTS sys_tables (
    table_id INTEGER NOT NULL PRIMARY KEY,
    table_name TEXT NOT NULL,
    table_url_string TEXT NOT NULL,
    schema_id INTEGER NOT NULL,
    partition_string TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (schema_id) REFERENCES sys_schemas(schema_id)
);

CREATE TABLE IF NOT EXISTS sys_schemas (
    schema_id INTEGER NOT NULL PRIMARY KEY,
    versions TEXT NOT NULL
    table_id INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    scheme_bin BLOB NOT NULL,
    FOREIGN KEY (table_id) REFERENCES sys_tables(table_id)
);
"#;

pub const INSERT_SYS_TABLES: &str =
    r#"
INSERT INTO sys_tables (table_id, table_name, table_url_string, schema_id, partition_string)
VALUES (?, ?, ?, ?, ?);
"#;

pub const DELETE_SYS_TABLES: &str = r#"
DELETE FROM sys_tables
WHERE table_id = ?;
"#;

pub const SELECT_SYS_TABLES: &str = r#"
SELECT * FROM sys_tables WHERE table_id = ?;
"#;


pub const SELECT_NAME_SYS_TABLES: &str = r#"
SELECT table_name FROM sys_tables ;
"#;

pub const INSERT_SYS_SCHEMAS: &str =
    r#"
INSERT INTO sys_schemas (schema_id, table_id, table_name, scheme_bin)
VALUES (?, ?, ?, ?);
"#;

pub const DELETE_SYS_SCHEMAS: &str = r#"
DELETE FROM sys_schemas
WHERE schema_id = ?;
"#;

pub const SELECT_SYS_SCHEMAS: &str = r#"
SELECT * FROM sys_schemas WHERE schema_id = ?;
"#;

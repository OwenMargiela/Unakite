pub const CREATE_SYSTEM_TABLE_SQL: &str =
    r#"
CREATE TABLE IF NOT EXISTS sys_tables (
    table_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    table_url_string TEXT NULL,
    schema_id INTEGER NOT NULL,
    partition_string TEXT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (schema_id) REFERENCES sys_schemas(schema_id)
) STRICT;

CREATE TABLE IF NOT EXISTS sys_schemas (
    schema_id INTEGER NOT NULL PRIMARY KEY  AUTOINCREMENT ,
    schema_bin BLOB NOT NULL,
    versions TEXT NULL,
    table_name TEXT NOT NULL,
    FOREIGN KEY (table_name) REFERENCES sys_tables(table_name)
) STRICT;
"#;

pub const INSERT_SYS_TABLES: &str =
    r#"
INSERT INTO sys_tables (table_name, schema_id)
VALUES ( ?, ?);
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
INSERT INTO sys_schemas (table_name, schema_bin)
VALUES (?, ?);
"#;

pub const DELETE_SYS_SCHEMAS: &str = r#"
DELETE FROM sys_schemas
WHERE table_id = ?;
"#;

pub const SELECT_SYS_SCHEMAS: &str = r#"
SELECT * FROM sys_schemas WHERE schema_id = ?;
"#;

pub const SELECT_SCHEMA_FROM_SYS_SCHEMA: &str =
    r#"
SELECT schema_bin FROM sys_schemas WHERE table_id = ?;
"#;

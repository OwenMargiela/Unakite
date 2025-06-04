// This file includes code adapted from the Convert CSV To Parquet (CC2P) project
// Repository: https://github.com/rayyildiz/cc2p
//
// Copyright (c) 2024 Ramazan AYYILDIZ
// Licensed under the MIT License.

// Original License:

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use glob::{ MatchOptions, glob_with };
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crate::utils::csv_tools::reader::CsvReader;

pub struct Empty {}
impl CsvReader {
    /// Converts a CSV file to Parquet format.
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path of the CSV file to be converted.
    /// * `delimiter` - The delimiter character used in the CSV file.
    /// * `has_header` - Indicates whether the CSV file has a header row.
    /// * `sampling_size` - The number of rows to sample for inferring the schema.
    ///
    /// # Returns
    ///
    /// Returns `Ok` if the conversion is successful, otherwise returns an `Err` with a `Box<dyn std::error::Error>`.
    ///
    ///
    ///
    /// ```

    pub fn convert_to_parquet(&self, sampling_size: u16) -> anyhow::Result<()> {
        let file = File::open(self.input.clone())?;

        let (csv_schema, _) = arrow_csv::reader::Format
            ::default()
            .with_header(self.has_header)
            .with_delimiter(self.delimiter as u8)
            .infer_schema(file, Some(sampling_size as usize))?;

        let schema_ref = CsvReader::remove_deduplicate_columns(csv_schema);

        let file = File::open(self.input.clone())?;
        let mut csv = arrow_csv::ReaderBuilder
            ::new(schema_ref.clone())
            .with_delimiter(self.delimiter as u8)
            .with_header(self.has_header)
            .build(file)?;

        let target_file = self.input.with_extension("parquet");

        // delete it if exist
        CsvReader::delete_if_exist(target_file.to_str().unwrap())?;

        let mut file = File::create(target_file).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_created_by("cc2p".to_string())
            .build();

        let mut parquet_writer = parquet::arrow::ArrowWriter::try_new(
            &mut file,
            schema_ref,
            Some(props)
        )?;

        for batch in csv.by_ref() {
            match batch {
                Ok(batch) => parquet_writer.write(&batch)?,
                Err(_error) => {
                    return Err(anyhow::Error::from_boxed(Box::new(_error)));
                }
            }
        }

        parquet_writer.close()?;

        Ok(())
    }

    pub fn partion_on(partitions: Vec<String>) -> anyhow::Result<()> {
        unimplemented!()
    }

    /// Deletes a file if it exists.
    ///
    /// # Arguments
    ///
    /// * `filename` - The name of the file to delete.
    ///
    /// # Errors
    ///
    /// Returns `Err` if there is an error accessing the file or deleting it.
    ///
    pub(self) fn delete_if_exist(filename: &str) -> anyhow::Result<()> {
        if fs::metadata(filename).is_ok() {
            fs::remove_file(filename)?;
        }

        Ok(())
    }

    /// Removes duplicate columns from a given Arrow schema, and returns a new schema with deduplicated columns.
    ///
    /// # Arguments
    ///
    /// * `sc` - The input Arrow schema.
    ///
    /// # Returns
    ///
    /// Returns an `Arc` containing the deduplicated schema.
    pub(self) fn remove_deduplicate_columns(sc: arrow_schema::Schema) -> Arc<arrow_schema::Schema> {
        let mut index = 1;
        let mut deduplicated_fields = Vec::new();
        let mut names = HashMap::new();
        for field in sc.fields() {
            let field_name = field.name().as_str();
            let field_name = CsvReader::clean_column_name(field_name);

            if let std::collections::hash_map::Entry::Vacant(e) = names.entry(field_name.clone()) {
                e.insert(Empty {});

                if field.name().is_empty() {
                    let name = format!("column_{}", index);
                    index += 1;
                    let new_field = <arrow_schema::Field as Clone>
                        ::clone(&(*field).clone())
                        .with_name(name);
                    deduplicated_fields.push(Arc::new(new_field));
                } else {
                    deduplicated_fields.push(field.clone());
                }
            } else {
                let name = format!("{}_{}", field_name, index);
                index += 1;
                let new_field = <arrow_schema::Field as Clone>
                    ::clone(&(*field).clone())
                    .with_name(name);
                deduplicated_fields.push(Arc::new(new_field));
            }
        }

        let list_fields: Vec<_> = deduplicated_fields.into_iter().collect();

        let deduplicated_schema = arrow_schema::Schema::new_with_metadata(list_fields, sc.metadata);

        Arc::new(deduplicated_schema)
    }

    /// Searches for files matching the given pattern.
    ///
    /// # Arguments
    ///
    /// * `pattern` - A string slice representing the search pattern.
    ///
    /// # Returns
    ///
    /// A vector of `PathBuf` representing the paths of the matching files.
    ///
    /// # Panics
    ///
    /// This function will panic if it fails to read the file search pattern.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::path::PathBuf;
    ///
    /// let pattern = "testdata/sample*.csv";
    /// let files = find_files(pattern);
    ///
    /// for file in files {
    ///     println!("{:?}", file);
    /// }
    /// ```
    pub(self) fn find_files(pattern: &str) -> Vec<PathBuf> {
        let mut files = vec![];
        let options = MatchOptions {
            case_sensitive: false,
            require_literal_separator: false,
            require_literal_leading_dot: false,
        };

        for entry in glob_with(pattern, options).expect("failed to read file search pattern") {
            match entry {
                Ok(p) => {
                    if p.is_file() {
                        if let Some(ext) = p.extension() {
                            if ext == "csv" {
                                files.push(p);
                            }
                        }
                    }
                }
                Err(e) => eprintln!("{:?}", e),
            }
        }

        files
    }

    /// Cleans a given string by removing any characters that are not alphanumeric or whitespace.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The string to be cleaned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cc2p::*;
    ///
    /// let name = clean_column_name("John!Doe");
    /// assert_eq!(name, "JohnDoe");
    ///
    /// let name = clean_column_name("Welcome, User 123!");
    /// assert_eq!(name, "Welcome User 123");
    /// ```
    ///
    /// # Returns
    ///
    /// A `String` containing the cleaned string, with all non-alphanumeric characters removed.
    pub(self) fn clean_column_name(column_name: &str) -> String {
        let cleaned = regex::Regex::new(r"[^a-zA-Z0-9_\-\s]").unwrap().replace_all(column_name, "");

        cleaned.to_string()
    }
}

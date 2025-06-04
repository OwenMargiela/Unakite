use std::path::PathBuf;

use crate::blob_writer::BackEnd;

pub struct CsvReader {
    pub(crate) input: PathBuf,
    pub(crate) back_end: BackEnd,

    pub(crate) make_partiotion_on: Option<Vec<String>>,
    pub(crate) has_header: bool,

    pub(crate) delimiter: char,
}

pub struct CsvReaderOps {
    input: PathBuf,
    back_end: BackEnd,

    make_partiotion_on: Option<Vec<String>>,
    has_header: bool,

    delimiter: char,
}

impl CsvReaderOps {
    pub fn make() -> Self {
        CsvReaderOps::default()
    }

    pub fn path(mut self, path: PathBuf) -> Self {
        self.input = path;
        self
    }

    pub fn storage_backend(mut self, storage: BackEnd) -> Self {
        self.back_end = storage;
        self
    }
    pub fn set_delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter;
        self
    }

    pub fn make_paritions(mut self, partitions_string: Vec<String>) -> Self {
        self.make_partiotion_on = Some(partitions_string);
        self
    }

    pub fn has_header(mut self, boolean: bool) -> Self {
        self.has_header = boolean;

        self
    }

    pub fn buiild(self) -> CsvReader {
        CsvReader {
            input: self.input,
            back_end: self.back_end,
            make_partiotion_on: self.make_partiotion_on,
            has_header: self.has_header,
            delimiter: self.delimiter,
        }
    }
}

impl Default for CsvReaderOps {
    fn default() -> Self {
        CsvReaderOps {
            input: PathBuf::new(),
            back_end: BackEnd::Local(PathBuf::new()),
            make_partiotion_on: None,
            has_header: true,
            delimiter: ',',
        }
    }
}

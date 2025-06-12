use std::path::PathBuf;

pub struct BlobWriter {
    pub(crate) input: PathBuf,

    pub(crate) make_partiotion_on: Option<Vec<String>>,
    pub(crate) has_header: bool,

    pub(crate) delimiter: char,
}

pub struct BlobWriterOps {
    input: PathBuf,

    make_partiotion_on: Option<Vec<String>>,
    has_header: bool,

    delimiter: char,
}

impl BlobWriterOps {
    pub fn make() -> Self {
        BlobWriterOps::default()
    }

    pub fn path(mut self, path: PathBuf) -> Self {
        self.input = path;
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

    pub fn buiild(self) -> BlobWriter {
        BlobWriter {
            input: self.input,
            make_partiotion_on: self.make_partiotion_on,
            has_header: self.has_header,
            delimiter: self.delimiter,
        }
    }
}

impl Default for BlobWriterOps {
    fn default() -> Self {
        BlobWriterOps {
            input: PathBuf::new(),

            make_partiotion_on: None,
            has_header: true,
            delimiter: ',',
        }
    }
}

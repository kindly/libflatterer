//! libflatterer - Library to make JSON flatterer.
//!
//! Mainly used for [flatterer](https://flatterer.opendata.coop/), which is a python library/cli using bindings to this library. Read [flatterer documentation](https://flatterer.opendata.coop/) to give high level overview of how the flattening works.
//! Nonetheless can be used as a standalone Rust library.
//!
//!
//!
//! High level usage, use flatten function, supply a BufReader, an output directory and the Options struct (generated with the builder pattern):
//! ```
//! use tempfile::TempDir;
//! use std::fs::File;
//! use libflatterer::{flatten, Options};
//! use std::io::BufReader;
//!
//! let tmp_dir = TempDir::new().unwrap();
//! let output_dir = tmp_dir.path().join("output");
//! let options = Options::builder().xlsx(true).sqlite(true).parquet(true).table_prefix("prefix_".into()).build();
//!
//! flatten(
//!    Box::new(BufReader::new(File::open("fixtures/basic.json").unwrap())), // reader
//!    output_dir.to_string_lossy().into(), // output directory
//!    options, // options
//! ).unwrap();
//!
//!```
//!
//! Lower level usage, use the `FlatFiles` struct directly and supply options.  
//!
//!```
//! use tempfile::TempDir;
//! use std::fs::File;
//! use libflatterer::{FlatFiles, Options};
//! use std::io::BufReader;
//! use serde_json::json;
//!
//! let myjson = json!({
//!     "a": "a",
//!     "c": ["a", "b", "c"],
//!     "d": {"da": "da", "db": "2005-01-01"},
//!     "e": [{"ea": "ee", "eb": "eb2"},
//!           {"ea": "ff", "eb": "eb2"}],
//! });
//!
//! let tmp_dir = TempDir::new().unwrap();
//! let output_dir = tmp_dir.path().join("output");
//! let options = Options::builder().xlsx(true).sqlite(true).parquet(true).table_prefix("prefix_".into()).build();
//!
//! // Create FlatFiles struct
//! let mut flat_files = FlatFiles::new(
//!     output_dir.to_string_lossy().into(), // output directory
//!     options
//! ).unwrap();
//!
//! // process JSON to memory
//! flat_files.process_value(myjson.clone(), vec![]);
//!
//! // write processed JSON to disk. Do not need to do this for every processed value, but it is recommended.
//! flat_files.create_rows();
//!
//! // copy the above two lines for each JSON object e.g..
//! flat_files.process_value(myjson.clone(), vec![]);
//! flat_files.create_rows();
//!
//! // ouput the selected formats
//! flat_files.write_files();
//!
//!```

mod guess_array;
mod postgresql;

#[cfg(not(target_family = "wasm"))]
mod schema_analysis;

#[cfg(not(target_family = "wasm"))]
mod yajlparser;

use indexmap::IndexMap as HashMap;
use indexmap::IndexSet as Set;

#[cfg(not(target_family = "wasm"))]
use std::convert::TryInto;
use std::fmt;
use std::fs::{create_dir_all, remove_dir_all, File};

#[cfg(not(target_family = "wasm"))]
use std::io::BufRead;
use std::io::{BufReader, Read, Write};

#[cfg(not(target_family = "wasm"))]
use std::io::{self};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(not(target_family = "wasm"))]
use std::{panic, thread};

#[cfg(not(target_family = "wasm"))]
use crossbeam_channel::{bounded, Receiver, SendError, Sender};
use csv::{ByteRecord, Reader, ReaderBuilder, Writer, WriterBuilder};

#[cfg(not(target_family = "wasm"))]
use csvs_convert::{
    datapackage_to_parquet_with_options, datapackage_to_postgres_with_options,
    datapackage_to_sqlite_with_options, datapackage_to_xlsx_with_options,
    merge_datapackage_with_options,
};

use csvs_convert::{Describer, DescriberOptions};

pub use guess_array::guess_array;
use itertools::Itertools;
use log::info;
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::{json, Deserializer, Map, Value};
use smallvec::{smallvec, SmallVec};
use smartstring::alias::String as SmartString;
use snafu::{Backtrace, ResultExt, Snafu};
use typed_builder::TypedBuilder;

use flate2::write::GzEncoder;
use flate2::Compression;
#[cfg(not(target_family = "wasm"))]
use xlsxwriter::Workbook;
#[cfg(not(target_family = "wasm"))]
use yajlish::Parser;
#[cfg(not(target_family = "wasm"))]
use yajlparser::Item;

use csv_async::{AsyncReaderBuilder, AsyncWriter, AsyncWriterBuilder};
#[cfg(not(target_family = "wasm"))]
use object_store::aws::AmazonS3Builder;
#[cfg(not(target_family = "wasm"))]
use object_store::path::Path;
#[cfg(not(target_family = "wasm"))]
use object_store::ObjectStore;
#[cfg(not(target_family = "wasm"))]
use parquet::arrow::async_writer::AsyncArrowWriter;
#[cfg(not(target_family = "wasm"))]
use tokio::io::AsyncWriteExt;
#[cfg(not(target_family = "wasm"))]
use tokio::io::{AsyncWrite, BufReader as AsycBufReader};
#[cfg(not(target_family = "wasm"))]
use tokio::sync::mpsc;
#[cfg(not(target_family = "wasm"))]
use tokio::runtime;
#[cfg(not(target_family = "wasm"))]
use async_compression::tokio::bufread::GzipDecoder;
use jsonpath_rust::{JsonPathFinder, JsonPathInst};


lazy_static::lazy_static! {
    pub static ref TERMINATE: AtomicBool = AtomicBool::new(false);
}

lazy_static::lazy_static! {
    #[allow(clippy::invalid_regex)]
    static ref INVALID_REGEX: regex::Regex = regex::RegexBuilder::new(r"[\000-\010]|[\013-\014]|[\016-\037]")
        .octal(true)
        .build()
        .unwrap();
}

#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not remove directory {}", filename))]
    FlattererRemoveDir {
        filename: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Could not create directory {}", filename))]
    FlattererCreateDir {
        filename: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Directory `{}` already exists.", dir.to_string_lossy()))]
    FlattererDirExists { dir: PathBuf },
    #[snafu(display(
        "Output XLSX will have too many rows {} in sheet `{}`, maximum allowed 1048576",
        rows,
        sheet
    ))]
    XLSXTooManyRows { rows: usize, sheet: String },
    #[snafu(display(
        "Output XLSX will have too many columns {} in sheet `{}`, maximum allowed 65536",
        columns,
        sheet
    ))]
    XLSXTooManyColumns { columns: usize, sheet: String },
    #[snafu(display("Terminated"))]
    Terminated {},
    #[cfg(not(target_family = "wasm"))]
    #[snafu(display("Json Dereferencing Failed"))]
    JSONRefError { source: schema_analysis::Error },
    #[snafu(display("Error writing to CSV file {}", filepath.to_string_lossy()))]
    FlattererCSVWriteError {
        filepath: PathBuf,
        source: csv::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Error writing to CSV file {}", filepath.to_string_lossy()))]
    FlattererCSVAsyncWriteError {
        filepath: PathBuf,
        source: csv_async::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Error reading file {}", filepath.to_string_lossy()))]
    FlattererReadError {
        filepath: PathBuf,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Error reading CSV file {}", filepath))]
    FlattererCSVReadError {
        filepath: String,
        source: csv::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Could not write file {}", filename))]
    FlattererFileWriteError {
        filename: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("IO Error: {}", source))]
    FlattererIoError {
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Could not write file {}", filename))]
    SerdeWriteError {
        filename: String,
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[cfg(not(target_family = "wasm"))]
    #[snafu(display("{}", source))]
    DatapackageConvertError {
        source: csvs_convert::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid JSON following error: {}", source))]
    SerdeReadError { source: serde_json::Error },
    #[snafu(display("Error: {}", message))]
    FlattererProcessError { message: String },
    #[snafu(display("Error: {}", message))]
    FlattererOptionError { message: String },
    #[snafu(display("{}", message))]
    FlattererOSError { message: String },
    #[cfg(not(target_family = "wasm"))]
    #[snafu(display("Error with writing XLSX file"))]
    FlattererXLSXError { source: xlsxwriter::XlsxError },
    #[snafu(display("Could not convert usize to int"))]
    FlattererIntError { source: std::num::TryFromIntError },
    #[snafu(display("YAJLish parse error: {}", error))]
    YAJLishParseError { error: String },
    #[cfg(not(target_family = "wasm"))]
    #[snafu(display(""))]
    ChannelSendError { source: SendError<Value> },
    #[cfg(not(target_family = "wasm"))]
    #[snafu(display(""))]
    TokioChannelSendError { source: tokio::sync::mpsc::error::SendError<Value> },
    #[cfg(not(target_family = "wasm"))]
    #[snafu(display(""))]
    ChannelItemError { source: SendError<Item> },
    #[cfg(not(target_family = "wasm"))]
    #[snafu(display(""))]
    TokioChannelItemError { source: tokio::sync::mpsc::error::SendError<Item> },
    #[cfg(not(target_family = "wasm"))]
    #[snafu(display(""))]
    ChannelBufSendError { source: SendError<(Vec<u8>, bool)> },
    #[cfg(not(target_family = "wasm"))]
    #[snafu(display(""))]
    ChannelStopSendError { source: SendError<()> },
    #[cfg(not(target_family = "wasm"))]
    #[snafu(display(""))]
    TokioChannelStopSendError { source: mpsc::error::SendError<()> },
    #[snafu(display(""))]
    FlattererCSVIntoInnerError {
        source: csv::IntoInnerError<Writer<flate2::write::GzEncoder<File>>>,
        backtrace: Backtrace,
    },
    #[snafu(display("{}", source))]
    ObjectStoreError {
        source: object_store::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{}", source))]
    JoinError {
        source: tokio::task::JoinError,
        backtrace: Backtrace,
    },
    #[snafu(display("{}", source))]
    URLError {
        source: url::ParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("{}", source))]
    ParquetError {
        source: parquet::errors::ParquetError,
        backtrace: Backtrace,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Expreses a JSON path element, either a string  or a number.
#[derive(Hash, Clone, Debug)]
enum PathItem {
    Key(SmartString),
    Index(usize),
}

impl fmt::Display for PathItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PathItem::Key(key) => write!(f, "{}", key),
            PathItem::Index(index) => write!(f, "{}", index),
        }
    }
}

enum TmpCSVWriter {
    #[cfg(not(target_family = "wasm"))]
    Disk(csv::Writer<File>),
    Memory(csv::Writer<GzEncoder<Vec<u8>>>),
    AsyncCSV(AsyncWriter<Box<dyn AsyncWrite + Unpin + Send>>),
    AsyncParquet(AsyncArrowWriter<Box<dyn AsyncWrite + Unpin + Send>>),
    None(),
}

#[derive(Default, Debug, TypedBuilder, Clone)]
pub struct Options {
    /// Output csv files. Default `true`
    #[builder(default = true)]
    pub csv: bool,
    /// Output xlsx
    #[builder(default)]
    pub xlsx: bool,
    /// Output sqlite
    #[builder(default)]
    pub sqlite: bool,
    /// Output parquet
    #[builder(default)]
    pub parquet: bool,
    /// Table name of main table, default "main"
    #[builder(default="main".into())]
    pub main_table_name: String,
    /// If array of objects always has one element in it then treat as one-to-one
    #[builder(default)]
    pub inline_one_to_one: bool,
    /// Prefix every table with supplied string,
    #[builder(default)]
    pub table_prefix: String,
    /// Seperator used for each element, Default to "_"
    #[builder(default = "_".into())]
    pub path_separator: String,
    /// Only output these amout of rows.
    #[builder(default)]
    pub preview: usize,
    /// Choose sqlite db file to create or append to
    #[builder(default)]
    pub sqlite_path: String,
    /// Prefix the main tables _link field with a prefix
    #[builder(default)]
    pub id_prefix: String,
    /// An array of paths where new tables are generated instead of inlined in parent table
    #[builder(default)]
    pub emit_obj: Vec<Vec<String>>,
    /// Delete output direcory if it exists
    #[builder(default)]
    pub force: bool,
    /// JSON schema that can be used for field order and field naming
    #[builder(default)]
    pub schema: String,
    /// Either "title", "underscore_slug" or "slug"
    #[builder(default)]
    pub schema_titles: String,
    /// Path to array of objects that will be treated as top level object
    #[builder(default)]
    pub path: Vec<String>,
    /// Data is concatonated JSON
    #[builder(default)]
    pub json_stream: bool,
    #[builder(default)]
    /// Data is new line delimeted JSON
    pub ndjson: bool,
    #[builder(default)]
    /// `tables.csv` file to use to name tables or remove tables from output
    pub tables_csv: String,
    #[builder(default)]
    /// `fields.csv` file to use to name fields or remove fields from output
    pub fields_csv: String,
    #[builder(default)]
    /// `tables.csv` file to use to name tables or remove tables from output
    pub tables_csv_string: String,
    #[builder(default)]
    /// `fields.csv` file to use to name fields or remove fields from output
    pub fields_csv_string: String,
    /// Only tables in `tables.csv` will be output
    #[builder(default)]
    pub only_tables: bool,
    /// Only fiels in `fields.csv` will be output
    #[builder(default)]
    pub only_fields: bool,
    /// Threads to use. Default 1.
    #[builder(default = 1)]
    pub threads: usize,
    /// Name of thread.
    #[builder(default)]
    pub thread_name: String,
    /// Pushdown fields to sub objects.
    #[builder(default)]
    pub pushdown: Vec<String>,
    /// postgres schema
    #[builder(default)]
    pub postgres_connection: String,
    /// drop tables
    #[builder(default)]
    pub drop: bool,
    /// postgres schema
    #[builder(default)]
    pub postgres_schema: String,
    /// sql scripts
    #[builder(default)]
    pub sql_scripts: bool,
    /// evolve data withing sqlite and postgres db
    #[builder(default)]
    pub evolve: bool,
    #[builder(default)]
    pub memory: bool,
    #[builder(default)]
    pub no_link: bool,
    #[builder(default)]
    pub stats: bool,
    #[builder(default)]
    pub s3: bool,
    #[builder(default)]
    pub low_disk: bool,
    #[builder(default)]
    pub gzip_input: bool,
    #[builder(default)]
    pub json_path_selector: String,
}

pub struct FlatFiles {
    pub options: Options,
    pub path: Vec<SmartString>,
    pub output_dir: PathBuf,
    main_table_name: String,
    emit_obj: SmallVec<[SmallVec<[String; 5]>; 5]>,
    row_number: usize,
    sub_array_row_number: usize,
    current_path: Vec<SmartString>,
    table_rows: HashMap<String, Vec<Map<String, Value>>>,
    tmp_csvs: HashMap<String, TmpCSVWriter>,
    table_metadata: HashMap<String, TableMetadata>,
    one_to_many_arrays: SmallVec<[SmallVec<[SmartString; 5]>; 5]>,
    one_to_one_arrays: SmallVec<[SmallVec<[SmartString; 5]>; 5]>,
    order_map: HashMap<String, usize>,
    field_titles_map: HashMap<String, String>,
    table_order: HashMap<String, String>,
    tmp_memory: HashMap<String, Vec<u8>>,
    pub csv_memory_gz: HashMap<String, Vec<u8>>,
    pub files_memory: HashMap<String, Vec<u8>>,
    direct: bool,
    object_store: Option<Box<dyn ObjectStore>>,
    json_path: Option<JsonPathFinder>,
}

#[derive(Serialize, Debug)]
struct TableMetadata {
    fields: Vec<String>,
    fields_set: Set<String>,
    field_counts: Vec<u32>,
    rows: usize,
    ignore: bool,
    ignore_fields: Vec<bool>,
    order: Vec<usize>,
    field_titles: Vec<String>,
    table_name_with_separator: String,
    output_path: PathBuf,
    field_titles_lc: Vec<String>,
    supplied_types: Vec<String>,
    #[serde(skip_serializing)]
    describers: Vec<Describer>,
    #[serde(skip_serializing)]
    arrow_vecs: Vec<ArrayBuilders>,
}

#[derive(Debug)]
enum ArrayBuilders {
    Boolean(Vec<Option<bool>>),
    Integer(Vec<Option<i64>>),
    Number(Vec<Option<f64>>),
    String(Vec<Option<String>>),
}

impl TableMetadata {
    fn new(
        table_name: &str,
        main_table_name: &str,
        path_separator: &str,
        table_prefix: &str,
        output_path: PathBuf,
    ) -> TableMetadata {
        let table_name_with_separator = if table_name == main_table_name {
            "".to_string()
        } else {
            let mut full_path = format!("{}{}", table_name, path_separator);
            if !table_prefix.is_empty() {
                full_path.replace_range(0..table_prefix.len(), "");
            }
            full_path
        };

        TableMetadata {
            fields: vec![],
            fields_set: Set::new(),
            field_counts: vec![],
            rows: 0,
            ignore: false,
            ignore_fields: vec![],
            order: vec![],
            field_titles: vec![],
            table_name_with_separator,
            output_path,
            field_titles_lc: vec![],
            describers: vec![],
            supplied_types: vec![],
            arrow_vecs: vec![],
        }
    }
}

#[derive(Debug, Deserialize)]
struct FieldsRecord {
    table_name: String,
    field_name: String,
    field_title: Option<String>,
    field_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TablesRecord {
    table_name: String,
    table_title: String,
}

#[cfg(not(target_family = "wasm"))]
struct JLWriter {
    pub buf: Vec<u8>,
    pub buf_sender: Sender<(Vec<u8>, bool)>,
}

#[cfg(not(target_family = "wasm"))]
impl Write for JLWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf == [b'\n'] {
            if self.buf_sender.send((self.buf.clone(), false)).is_err() {
                log::error!(
                    "Unable to process any data, most likely caused by termination of worker"
                );
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unable to process any data, most likely caused by termination of worker",
                ));
            }
            self.buf.clear();
            Ok(buf.len())
        } else {
            self.buf.extend_from_slice(buf);
            Ok(buf.len())
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl FlatFiles {
    pub fn new_with_defaults(output_dir: String) -> Result<Self> {
        let options = Options::builder().build();
        Self::new(output_dir, options)
    }

    pub fn new(output_dir: String, mut options: Options) -> Result<Self> {
        #[cfg(target_family = "wasm")]
        {
            options.memory = true;
        }

        if !options.memory && !options.s3 {
            let output_path = PathBuf::from(output_dir.clone());
            if output_path.is_dir() {
                if options.force {
                    remove_dir_all(&output_path).context(FlattererRemoveDirSnafu {
                        filename: output_path.to_string_lossy(),
                    })?;
                } else {
                    return Err(Error::FlattererDirExists { dir: output_path });
                }
            }

            let tmp_path = output_path.join("tmp");
            create_dir_all(&tmp_path).context(FlattererCreateDirSnafu {
                filename: tmp_path.to_string_lossy(),
            })?;
        }

        let smallvec_emit_obj: SmallVec<[SmallVec<[String; 5]>; 5]> = smallvec![];

        let main_table_name = [
            options.table_prefix.clone(),
            options.main_table_name.clone(),
        ]
        .concat();

        #[cfg(not(target_family = "wasm"))]
        if options.evolve && options.id_prefix.is_empty() {
            options.id_prefix = format!("{}.", nanoid::nanoid!(10));
        }

        let path = options.path.iter().map(SmartString::from).collect_vec();

        let direct = (!options.fields_csv.is_empty() || !options.fields_csv_string.is_empty())
            && options.only_fields;

        let object_store: Option<Box<dyn ObjectStore>> = if !options.s3 {
            None
        } else {
            let s3 = AmazonS3Builder::from_env()
                .with_retry(object_store::RetryConfig {
                    max_retries: 5,
                    ..Default::default()
                })
                .with_url(&output_dir)
                .with_client_options(
                     object_store::ClientOptions::new()
                     .with_http1_only()
                     .with_pool_max_idle_per_host(0)
                //       .with_connect_timeout(std::time::Duration::from_secs(10))
                //        .with_timeout(std::time::Duration::from_secs(10))
                 )
                .build()
                .context(ObjectStoreSnafu {})?;
            Some(Box::new(s3))
        };

        let mut new_output_dir = output_dir.clone();

        if options.s3 {
            let url = url::Url::parse(&output_dir).context(URLSnafu {})?;
            new_output_dir = url.path().to_string();
            new_output_dir.remove(0);
        }

        let json_path = if options.json_path_selector.is_empty() {
            None
        } else {
            let jsonpath = JsonPathInst::from_str(&options.json_path_selector).unwrap();
            Some(JsonPathFinder::new(Box::new(json!({})), Box::new(jsonpath)))
        }; 

        let mut flat_files = Self {
            output_dir: new_output_dir.into(),
            options,
            path,
            main_table_name,
            emit_obj: smallvec_emit_obj,
            row_number: 0,
            sub_array_row_number: 0,
            current_path: vec![],
            table_rows: HashMap::new(),
            tmp_csvs: HashMap::new(),
            table_metadata: HashMap::new(),
            one_to_many_arrays: SmallVec::new(),
            one_to_one_arrays: SmallVec::new(),
            order_map: HashMap::new(),
            field_titles_map: HashMap::new(),
            table_order: HashMap::new(),
            tmp_memory: HashMap::new(),
            csv_memory_gz: HashMap::new(),
            files_memory: HashMap::new(),
            direct,
            object_store,
            json_path
        };

        #[cfg(not(target_family = "wasm"))]
        if !flat_files.options.s3 {
            flat_files.create_csv_dir()?;
        }

        #[cfg(not(target_family = "wasm"))]
        if !flat_files.options.schema.is_empty() {
            flat_files.set_schema()?;
        };

        flat_files.set_emit_obj()?;

        if !flat_files.options.tables_csv.is_empty()
            || !flat_files.options.tables_csv_string.is_empty()
        {
            flat_files.use_tables_csv()?;
        };

        if !flat_files.options.fields_csv.is_empty()
            || !flat_files.options.fields_csv_string.is_empty()
        {
            flat_files.use_fields_csv()?;
        };

        Ok(flat_files)
    }

    #[cfg(not(target_family = "wasm"))]
    fn create_csv_dir(&mut self) -> Result<()> {
        let csv_path = self.output_dir.join("csv");
        if !csv_path.is_dir() {
            create_dir_all(&csv_path).context(FlattererCreateDirSnafu {
                filename: csv_path.to_string_lossy(),
            })?;
        }

        Ok(())
    }

    #[cfg(not(target_family = "wasm"))]
    fn set_schema(&mut self) -> Result<()> {
        let schema_analysis = schema_analysis::schema_analysis(
            &self.options.schema,
            &self.options.path_separator,
            self.options.schema_titles.clone(),
        )
        .context(JSONRefSnafu {})?;
        self.order_map = schema_analysis.field_order_map;
        self.field_titles_map = schema_analysis.field_titles_map;
        Ok(())
    }

    fn set_emit_obj(&mut self) -> Result<()> {
        for emit_vec in &self.options.emit_obj {
            self.emit_obj.push(SmallVec::from_vec(emit_vec.clone()))
        }
        Ok(())
    }

    fn handle_obj(
        &mut self,
        mut obj: Map<String, Value>,
        emit: bool,
        full_path: SmallVec<[PathItem; 10]>,
        no_index_path: SmallVec<[SmartString; 5]>,
        one_to_many_full_paths: SmallVec<[SmallVec<[PathItem; 10]>; 5]>,
        one_to_many_no_index_paths: SmallVec<[SmallVec<[SmartString; 5]>; 5]>,
        parent_one_to_one_key: bool,
        pushdown_keys: SmallVec<[String; 10]>,
        pushdown_values: SmallVec<[Value; 10]>,
    ) -> Option<Map<String, Value>> {
        let mut table_name = String::new();

        if emit {
            self.set_table_name(&mut table_name, &no_index_path);

            if !self.table_rows.contains_key(&table_name) {
                self.table_rows.insert(table_name.clone(), vec![]);
            }
        }

        let mut to_insert: SmallVec<[(String, Value); 30]> = smallvec![];
        let mut to_delete: SmallVec<[String; 30]> = smallvec![];

        let mut one_to_one_array: SmallVec<[(String, Value); 30]> = smallvec![];

        let mut new_pushdown_keys = pushdown_keys.clone();
        let mut new_pushdown_values = pushdown_values.clone();

        for key in self.options.pushdown.iter() {
            if let Some(value) = obj.get(key) {
                if table_name.is_empty() {
                    self.set_table_name(&mut table_name, &no_index_path);
                }
                new_pushdown_keys.push(
                    [
                        table_name.clone(),
                        self.options.path_separator.clone(),
                        key.clone(),
                    ]
                    .concat(),
                );
                new_pushdown_values.push(value.clone());
            }
        }

        for (key, value) in obj.iter_mut() {
            if let Some(arr) = value.as_array() {
                let mut str_count = 0;
                let mut obj_count = 0;
                let arr_length = arr.len();
                for array_value in arr {
                    if array_value.is_object() {
                        obj_count += 1
                    };
                    if array_value.is_string() {
                        str_count += 1
                    };
                }
                if arr_length == 0 {
                    to_delete.push(key.clone());
                } else if str_count == arr_length {
                    let keys: Vec<String> = arr
                        .iter()
                        .map(|val| (val.as_str().unwrap().to_string())) //value known as str
                        .collect();
                    let mut csv_bytes = vec![];
                    {
                        let mut csv_writer = csv::Writer::from_writer(&mut csv_bytes);
                        csv_writer.write_record(keys).unwrap(); // safe as just writing to vec
                    }
                    let values = String::from_utf8_lossy(&csv_bytes);
                    let trimmed = values.trim();
                    let trimmed_value = json!(trimmed);
                    to_insert.push((key.clone(), trimmed_value))
                } else if obj_count == arr_length {
                    to_delete.push(key.clone());
                    let mut removed_array = value.take(); // obj.remove(&key).unwrap(); //key known
                    let my_array = removed_array.as_array_mut().unwrap(); //key known as array
                    for (i, array_value) in my_array.iter_mut().enumerate() {
                        let my_value = array_value.take();
                        let mut new_full_path = full_path.clone();
                        new_full_path.push(PathItem::Key(SmartString::from(key)));
                        new_full_path.push(PathItem::Index(i));

                        let mut new_one_to_many_full_paths = one_to_many_full_paths.clone();
                        new_one_to_many_full_paths.push(new_full_path.clone());

                        let mut new_no_index_path = no_index_path.clone();
                        new_no_index_path.push(SmartString::from(key));

                        let mut new_one_to_many_no_index_paths = one_to_many_no_index_paths.clone();
                        new_one_to_many_no_index_paths.push(new_no_index_path.clone());

                        if self.options.inline_one_to_one
                            && !self.one_to_many_arrays.contains(&new_no_index_path)
                        {
                            if arr_length == 1 {
                                one_to_one_array.push((key.clone(), my_value.clone()));
                                if !self.one_to_one_arrays.contains(&new_no_index_path) {
                                    self.one_to_one_arrays.push(new_no_index_path.clone())
                                }
                            } else {
                                self.one_to_one_arrays.retain(|x| x != &new_no_index_path);
                                self.one_to_many_arrays.push(new_no_index_path.clone())
                            }
                        }

                        if let Value::Object(my_obj) = my_value {
                            if !parent_one_to_one_key && !my_obj.is_empty() {
                                self.handle_obj(
                                    my_obj,
                                    true,
                                    new_full_path,
                                    new_no_index_path,
                                    new_one_to_many_full_paths,
                                    new_one_to_many_no_index_paths,
                                    false,
                                    new_pushdown_keys.clone(),
                                    new_pushdown_values.clone(),
                                );
                            }
                        }
                    }
                } else {
                    let json_value = json!(format!("{}", value));
                    to_insert.push((key.clone(), json_value));
                }
            }
        }

        let mut one_to_one_array_keys = vec![];

        for (key, value) in one_to_one_array {
            one_to_one_array_keys.push(key.clone());
            obj.insert(key, value);
        }

        for (key, value) in obj.iter_mut() {
            if value.is_object() {
                let my_value = value.take();
                to_delete.push(key.clone());

                let mut new_full_path = full_path.clone();
                new_full_path.push(PathItem::Key(SmartString::from(key)));
                let mut new_no_index_path = no_index_path.clone();
                new_no_index_path.push(SmartString::from(key));

                let mut emit_child = false;
                if self
                    .emit_obj
                    .iter()
                    .any(|emit_path| emit_path == &new_no_index_path)
                {
                    emit_child = true;
                }
                if let Value::Object(my_value) = my_value {
                    let new_obj = self.handle_obj(
                        my_value,
                        emit_child,
                        new_full_path,
                        new_no_index_path,
                        one_to_many_full_paths.clone(),
                        one_to_many_no_index_paths.clone(),
                        one_to_one_array_keys.contains(key),
                        new_pushdown_keys.clone(),
                        new_pushdown_values.clone(),
                    );
                    if let Some(mut my_obj) = new_obj {
                        for (new_key, new_value) in my_obj.iter_mut() {
                            let mut object_key = String::with_capacity(100);
                            object_key.push_str(key);
                            object_key.push_str(&self.options.path_separator);
                            object_key.push_str(new_key);

                            to_insert.push((object_key, new_value.take()));
                        }
                    }
                }
            }
        }
        for key in to_delete {
            obj.remove(&key);
        }
        for (key, value) in to_insert {
            obj.insert(key, value);
        }

        if emit {
            for (key, value) in pushdown_keys.iter().zip(pushdown_values) {
                if !value.is_null() && !obj.contains_key(key) {
                    obj.insert(key.to_owned(), value);
                }
            }
            self.process_obj(
                obj,
                table_name,
                one_to_many_full_paths,
                one_to_many_no_index_paths,
            );
            None
        } else {
            Some(obj)
        }
    }

    fn set_table_name(&self, table_name: &mut String, no_index_path: &SmallVec<[SmartString; 5]>) {
        *table_name = [
            self.options.table_prefix.clone(),
            no_index_path.join(&self.options.path_separator),
        ]
        .concat();
        if no_index_path.is_empty() {
            *table_name = self.main_table_name.clone();
        }
    }

    fn process_obj(
        &mut self,
        mut obj: Map<String, Value>,
        table_name: String,
        one_to_many_full_paths: SmallVec<[SmallVec<[PathItem; 10]>; 5]>,
        one_to_many_no_index_paths: SmallVec<[SmallVec<[SmartString; 5]>; 5]>,
    ) {
        if !self.options.no_link {
            self.insert_link_fields(
                one_to_many_full_paths,
                one_to_many_no_index_paths,
                &mut obj,
                &table_name,
            );
        }
        let current_list = self.table_rows.get_mut(&table_name).unwrap(); //we added table_row already
        current_list.push(obj);
    }

    fn insert_link_fields(
        &mut self,
        one_to_many_full_paths: SmallVec<[SmallVec<[PathItem; 10]>; 5]>,
        one_to_many_no_index_paths: SmallVec<
            [SmallVec<[smartstring::SmartString<smartstring::LazyCompact>; 5]>; 5],
        >,
        obj: &mut Map<String, Value>,
        table_name: &String,
    ) {
        let mut path_iter = one_to_many_full_paths
            .iter()
            .zip(one_to_many_no_index_paths)
            .peekable();

        if one_to_many_full_paths.is_empty() {
            obj.insert(
                String::from("_link"),
                Value::String(
                    [
                        self.options.id_prefix.to_owned(),
                        self.row_number.to_string(),
                    ]
                    .concat(),
                ),
            );
        } else {
            obj.insert(
                String::from("_link"),
                Value::String(
                    [
                        self.options.id_prefix.to_owned(),
                        self.row_number.to_string(),
                        ".".to_string(),
                        one_to_many_full_paths[one_to_many_full_paths.len() - 1]
                            .iter()
                            .join("."),
                    ]
                    .concat(),
                ),
            );
        }

        while let Some((full, no_index)) = path_iter.next() {
            if path_iter.peek().is_some() {
                obj.insert(
                    [
                        "_link_".to_string(),
                        self.options.table_prefix.clone(),
                        no_index.iter().join(&self.options.path_separator),
                    ]
                    .concat(),
                    Value::String(
                        [
                            self.options.id_prefix.to_owned(),
                            self.row_number.to_string(),
                            ".".to_string(),
                            full.iter().join("."),
                        ]
                        .concat(),
                    ),
                );
            } else {
                obj.insert(
                    String::from("_link"),
                    Value::String(
                        [
                            self.options.id_prefix.to_owned(),
                            self.row_number.to_string(),
                            ".".to_string(),
                            full.iter().join("."),
                        ]
                        .concat(),
                    ),
                );
            }
        }

        if *table_name != self.main_table_name {
            obj.insert(
                ["_link_", &self.main_table_name].concat(),
                Value::String(
                    [
                        self.options.id_prefix.to_owned(),
                        self.row_number.to_string(),
                    ]
                    .concat(),
                ),
            );
        };
    }

    pub fn create_rows(&mut self) -> Result<()> {
        for (table, rows) in self.table_rows.iter_mut() {
            if !self.tmp_csvs.contains_key(table) {
                if self.options.csv
                    || self.options.xlsx
                    || self.options.sqlite
                    || self.options.parquet
                    || !self.options.postgres_connection.is_empty()
                {
                    if self.options.memory {
                        let encoder = GzEncoder::new(vec![], Compression::default());
                        self.tmp_csvs.insert(
                            table.clone(),
                            TmpCSVWriter::Memory(
                                WriterBuilder::new()
                                    .flexible(!self.direct)
                                    .from_writer(encoder),
                            ),
                        );
                    } else {
                        #[cfg(not(target_family = "wasm"))]
                        let output_path = self.output_dir.join(format!(
                            "{}/{}.csv",
                            if self.direct { "csv" } else { "tmp" },
                            table
                        ));
                        let mut writer = WriterBuilder::new()
                            .flexible(!self.direct)
                            .from_writer(get_writer_from_path(&output_path)?);
                        if self.direct {
                            writer
                                .write_record(
                                    self.table_metadata.get(table).unwrap().field_titles.clone(),
                                ).context(FlattererCSVWriteSnafu {filepath: &output_path})?
                        }
                        #[cfg(not(target_family = "wasm"))]
                        self.tmp_csvs
                            .insert(table.clone(), TmpCSVWriter::Disk(writer));
                    }
                } else {
                    self.tmp_csvs.insert(table.clone(), TmpCSVWriter::None());
                }
            }

            if !self.table_metadata.contains_key(table) {
                let output_path = self.output_dir.join(format!("tmp/{}.csv", table));
                self.table_metadata.insert(
                    table.clone(),
                    TableMetadata::new(
                        table,
                        &self.main_table_name,
                        &self.options.path_separator,
                        &self.options.table_prefix,
                        output_path,
                    ),
                );
                if !self.options.only_tables && !self.table_order.contains_key(table) {
                    self.table_order.insert(table.clone(), table.clone());
                }
            }

            let table_metadata = self.table_metadata.get_mut(table).unwrap(); //key known
            let writer = self.tmp_csvs.get_mut(table).unwrap(); //key known

            for row in rows {
                let mut output_row: SmallVec<[String; 30]> = smallvec![];
                for (num, field) in table_metadata.fields.iter().enumerate() {
                    if let Some(value) = row.get_mut(field) {
                        table_metadata.field_counts[num] += 1;
                        let string_value =
                            value_convert(value.take(), &mut table_metadata.describers, num);
                        output_row.push(string_value);
                    } else {
                        output_row.push("".to_string());
                    }
                }
                for (key, value) in row {
                    if !table_metadata.fields_set.contains(key) && !self.options.only_fields {
                        table_metadata.fields.push(key.clone());
                        table_metadata.fields_set.insert(key.clone());
                        table_metadata.field_counts.push(1);
                        table_metadata.ignore_fields.push(false);
                        let options = DescriberOptions::builder()
                            .force_string(!self.options.no_link && key.starts_with("_link"))
                            .stats(self.options.stats && self.options.threads == 1)
                            .build();
                        table_metadata
                            .describers
                            .push(Describer::new_with_options(options));
                        let full_path =
                            format!("{}{}", table_metadata.table_name_with_separator, key);

                        if let Some(title) = self.field_titles_map.get(&full_path) {
                            table_metadata.field_titles.push(title.clone());
                        } else {
                            table_metadata.field_titles.push(key.clone());
                        }

                        output_row.push(value_convert(
                            value.take(),
                            &mut table_metadata.describers,
                            table_metadata.fields.len() - 1,
                        ));
                    }
                }
                if !output_row.is_empty() {
                    table_metadata.rows += 1;

                    #[cfg(not(target_family = "wasm"))]
                    if let TmpCSVWriter::Disk(writer) = writer {
                        writer
                            .write_record(&output_row)
                            .context(FlattererCSVWriteSnafu {
                                filepath: &table_metadata.output_path,
                            })?;
                    }
                    if let TmpCSVWriter::Memory(writer) = writer {
                        writer
                            .write_record(&output_row)
                            .context(FlattererCSVWriteSnafu {
                                filepath: &table_metadata.output_path,
                            })?;
                    }
                }
            }
        }
        for val in self.table_rows.values_mut() {
            val.clear();
        }
        Ok(())
    }

    pub async fn create_arrow_cols(&mut self) -> Result<()> {
        for (table, rows) in self.table_rows.iter_mut() {
            let table_metadata = self.table_metadata.get_mut(table).unwrap(); //key known

            for row in rows {
                for (num, field) in table_metadata.fields.iter().enumerate() {
                    let value_option = row.get_mut(field);
                    table_metadata.field_counts[num] += 1;

                    match &mut table_metadata.arrow_vecs[num] {
                        ArrayBuilders::Boolean(b) => {
                            if value_option.is_none() {
                                b.push(None);
                                continue;
                            }
                            let value = value_option.expect("checked above");
                            match value {
                                Value::Null => b.push(None),
                                Value::Bool(bool) => b.push(Some(*bool)),
                                Value::String(str) => {
                                    match str.parse::<bool>() {
                                        Ok(bool) => b.push(Some(bool)),
                                        Err(_) => return Err(Error::FlattererProcessError {
                                            message: format!(
                                                "Unable to parse {str} as bool in field {field}"
                                            ),
                                        }),
                                    }
                                }
                                _ => {
                                    return Err(Error::FlattererProcessError {
                                        message: format!(
                                            "Unable to parse {value} as bool in field {field}"
                                        ),
                                    })
                                }
                            }
                        }
                        ArrayBuilders::Integer(integer) => {
                            if value_option.is_none() {
                                integer.push(None);
                                continue;
                            }
                            let value = value_option.expect("checked above");
                            match value {
                                Value::Null => integer.push(None),
                                Value::Number(num) => integer.push(Some(num.as_i64().ok_or(
                                    Error::FlattererProcessError {
                                        message: format!(
                                            "Unable to parse {num} as integer in field {field}"
                                        ),
                                    },
                                )?)),
                                Value::String(str) => {
                                    match str.parse::<i64>() {
                                        Ok(parsed) => integer.push(Some(parsed)),
                                        Err(_) => return Err(Error::FlattererProcessError {
                                            message: format!(
                                                "Unable to parse {str} as integer in field {field}"
                                            ),
                                        }),
                                    }
                                }
                                _ => {
                                    return Err(Error::FlattererProcessError {
                                        message: format!(
                                            "Unable to parse {value} as integer in field {field}"
                                        ),
                                    })
                                }
                            }
                        }
                        ArrayBuilders::String(string) => {
                            if value_option.is_none() {
                                string.push(None);
                                continue;
                            }
                            let value = value_option.expect("checked above");
                            match value {
                                Value::Null => string.push(None),
                                Value::String(str) => string.push(Some(str.to_string())),
                                val => string.push(Some(val.to_string())),
                            }
                        }
                        ArrayBuilders::Number(number) => {
                            if value_option.is_none() {
                                number.push(None);
                                continue;
                            }
                            let value = value_option.expect("checked above");
                            match value {
                                Value::Null => number.push(None),
                                Value::String(str) => number.push(Some(str.parse().unwrap())),
                                Value::Number(num) => number.push(Some(num.as_f64().ok_or(
                                    Error::FlattererProcessError {
                                        message: format!(
                                            "Unable to parse {num} as float in field {field}"
                                        ),
                                    },
                                )?)),
                                _ => return Err(Error::FlattererProcessError {
                                        message: format!(
                                            "Unable to parse {value} as number in field {field}"
                                        ),
                                    })
                            }
                        }
                    }
                }
            }

            let mut arrow_arrays: Vec<(&String, arrow_array::array::ArrayRef, bool)> = vec![];

            use std::sync::Arc;

            for (index, array_builder) in table_metadata.arrow_vecs.iter_mut().enumerate() {
                let field_name = &table_metadata.field_titles[index];
                match array_builder {
                    ArrayBuilders::String(b) => {
                        let array = Arc::new(arrow_array::array::LargeStringArray::from_iter(b.drain(..)));
                        arrow_arrays.push((field_name, array, true))
                    }
                    ArrayBuilders::Boolean(b) => {
                        let array = Arc::new(arrow_array::array::BooleanArray::from_iter(b.drain(..)));
                        arrow_arrays.push((field_name, array, true))
                    }
                    ArrayBuilders::Integer(b) => {
                        let array = Arc::new(arrow_array::array::Int64Array::from_iter(b.drain(..)));
                        arrow_arrays.push((field_name, array, true))
                    }
                    ArrayBuilders::Number(b) => {
                        let array = Arc::new(arrow_array::array::Float64Array::from_iter(b.drain(..)));
                        arrow_arrays.push((field_name, array, true))
                    }
                }
            }
            let record_batch = arrow_array::RecordBatch::try_from_iter_with_nullable(arrow_arrays)
                .expect("should be well formed arrays");


            if !self.tmp_csvs.contains_key(table) {
                let file_path = format!(
                    "{}/parquet/{}.parquet",
                    self.output_dir.to_string_lossy(),
                    table
                );

                let path: Path = Path::from(file_path);
                let (_id, writer) = self
                    .object_store
                    .as_ref()
                    .expect("object store should exist")
                    .put_multipart(&path)
                    .await
                    .context(ObjectStoreSnafu {})?;

                let props = parquet::file::properties::WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::SNAPPY)
                    .set_max_row_group_size(1024 * 50)
                    .build();

                let parquet_writer =
                    AsyncArrowWriter::try_new(writer, record_batch.schema(), 1024, Some(props)).context(ParquetSnafu {})?;

                self.tmp_csvs
                    .insert(table.clone(), TmpCSVWriter::AsyncParquet(parquet_writer));
            }

            let writer = self.tmp_csvs.get_mut(table).unwrap(); //key known

            if let TmpCSVWriter::AsyncParquet(parquet_writer) = writer {
                parquet_writer.write(&record_batch).await.context(ParquetSnafu {})?;
            }

        }
        for val in self.table_rows.values_mut() {
            val.clear();
        }
        Ok(())
    }

    pub async fn create_rows_async(&mut self) -> Result<()> {
        for (table, rows) in self.table_rows.iter_mut() {
            if !self.tmp_csvs.contains_key(table) {
                if self.options.csv {

                    let path = format!(
                        "{}/{}/{}.csv",
                        self.output_dir.to_string_lossy(),
                        if self.direct { "csv" } else { "tmp" },
                        table
                    );
                    
                    let (_id, writer) = self
                        .object_store
                        .as_ref()
                        .expect("object store should exist")
                        .put_multipart(&path.into())
                        .await
                        .context(ObjectStoreSnafu {})?;

                    let csv_writer = AsyncWriterBuilder::new()
                        .flexible(!self.direct)
                        .create_writer(writer);

                    self.tmp_csvs
                        .insert(table.clone(),  TmpCSVWriter::AsyncCSV(csv_writer));
                } else {
                    self.tmp_csvs.insert(table.clone(), TmpCSVWriter::None());
                }
            }

            if !self.table_metadata.contains_key(table) {
                let output_path = self.output_dir.join(format!("tmp/{}.csv", table));
                self.table_metadata.insert(
                    table.clone(),
                    TableMetadata::new(
                        table,
                        &self.main_table_name,
                        &self.options.path_separator,
                        &self.options.table_prefix,
                        output_path,
                    ),
                );
                if !self.options.only_tables && !self.table_order.contains_key(table) {
                    self.table_order.insert(table.clone(), table.clone());
                }
            }

            let table_metadata = self.table_metadata.get_mut(table).unwrap(); //key known
            let writer = self.tmp_csvs.get_mut(table).unwrap(); //key known

            for row in rows {
                let mut output_row: SmallVec<[String; 30]> = smallvec![];
                for (num, field) in table_metadata.fields.iter().enumerate() {
                    if let Some(value) = row.get_mut(field) {
                        table_metadata.field_counts[num] += 1;
                        let string_value =
                            value_convert(value.take(), &mut table_metadata.describers, num);
                        output_row.push(string_value);
                    } else {
                        output_row.push("".to_string());
                    }
                }
                for (key, value) in row {
                    if !table_metadata.fields_set.contains(key) && !self.options.only_fields {
                        table_metadata.fields.push(key.clone());
                        table_metadata.fields_set.insert(key.clone());
                        table_metadata.field_counts.push(1);
                        table_metadata.ignore_fields.push(false);
                        let options = DescriberOptions::builder()
                            .force_string(!self.options.no_link && key.starts_with("_link"))
                            .stats(self.options.stats && self.options.threads == 1)
                            .build();
                        table_metadata
                            .describers
                            .push(Describer::new_with_options(options));
                        let full_path =
                            format!("{}{}", table_metadata.table_name_with_separator, key);

                        if let Some(title) = self.field_titles_map.get(&full_path) {
                            table_metadata.field_titles.push(title.clone());
                        } else {
                            table_metadata.field_titles.push(key.clone());
                        }

                        output_row.push(value_convert(
                            value.take(),
                            &mut table_metadata.describers,
                            table_metadata.fields.len() - 1,
                        ));
                    }
                }
                if !output_row.is_empty() {
                    table_metadata.rows += 1;

                    #[cfg(not(target_family = "wasm"))]
                    if let TmpCSVWriter::AsyncCSV(writer) = writer {
                        writer.write_record(&output_row).await.context(
                            FlattererCSVAsyncWriteSnafu {
                                filepath: &table_metadata.output_path,
                            },
                        )?;
                    }
                    if let TmpCSVWriter::Memory(writer) = writer {
                        writer
                            .write_record(&output_row)
                            .context(FlattererCSVWriteSnafu {
                                filepath: &table_metadata.output_path,
                            })?;
                    }
                }
            }
        }
        for val in self.table_rows.values_mut() {
            val.clear();
        }
        Ok(())
    }

    pub fn process_value(&mut self, value: Value, initial_path: Vec<SmartString>) {

        if let Some(json_path) = self.json_path.as_mut() {
            json_path.set_json(Box::new(value.clone()));
            let output = json_path.find_slice();
            for result in output {
                if let jsonpath_rust::JsonPathValue::NoValue = result {
                    return;
                }
            }
        }

        if let Value::Object(obj) = value {
            let has_path = !initial_path.is_empty();
            if initial_path != self.current_path {
                self.sub_array_row_number = 0;
                self.current_path = initial_path.clone();
            }
            let mut full_path =
                SmallVec::from_iter(initial_path.iter().map(|item| PathItem::Key(item.clone())));
            let no_index_path = SmallVec::from_vec(initial_path);
            let mut one_to_many_full_paths = SmallVec::new();
            let mut one_to_many_no_index_paths = SmallVec::new();

            if has_path {
                full_path.push(PathItem::Index(self.sub_array_row_number));
                one_to_many_full_paths.push(full_path.clone());
                one_to_many_no_index_paths.push(no_index_path.clone());
            }

            self.handle_obj(
                obj,
                true,
                full_path,
                no_index_path,
                one_to_many_full_paths,
                one_to_many_no_index_paths,
                false,
                smallvec![],
                smallvec![],
            );
            if has_path {
                self.sub_array_row_number += 1;
            } else {
                self.row_number += 1;
            }
        }
    }

    pub fn use_tables_csv(&mut self) -> Result<()> {
        let reader: Box<dyn Read>;

        if self.options.tables_csv_string.is_empty() {
            reader = Box::new(File::open(&self.options.tables_csv).context(
                FlattererReadSnafu {
                    filepath: PathBuf::from(&self.options.tables_csv),
                },
            )?);
        } else {
            reader = Box::new(self.options.tables_csv_string.as_bytes());
        }

        let mut tables_reader = Reader::from_reader(reader);

        for row in tables_reader.deserialize() {
            let row: TablesRecord = row.context(FlattererCSVReadSnafu {
                filepath: &self.options.tables_csv,
            })?;
            self.table_order.insert(row.table_name, row.table_title);
        }
        Ok(())
    }

    pub fn use_fields_csv(&mut self) -> Result<()> {
        let reader: Box<dyn Read>;

        if self.options.fields_csv_string.is_empty() {
            reader = Box::new(File::open(&self.options.fields_csv).context(
                FlattererReadSnafu {
                    filepath: PathBuf::from(&self.options.fields_csv),
                },
            )?);
        } else {
            reader = Box::new(self.options.fields_csv_string.as_bytes());
        }

        let mut fields_reader = Reader::from_reader(reader);

        for row in fields_reader.deserialize() {
            let row: FieldsRecord = row.context(FlattererCSVReadSnafu {
                filepath: &self.options.fields_csv,
            })?;

            if !self.table_metadata.contains_key(&row.table_name) {
                let output_path = self.output_dir.join(format!("tmp/{}.csv", &row.table_name));
                self.table_metadata.insert(
                    row.table_name.clone(),
                    TableMetadata::new(
                        &row.table_name,
                        &self.main_table_name,
                        &self.options.path_separator,
                        &self.options.table_prefix,
                        output_path,
                    ),
                );
                if !self.options.only_tables {
                    self.table_order
                        .insert(row.table_name.clone(), row.table_name.clone());
                }
            }
            let table_metadata = self.table_metadata.get_mut(&row.table_name).unwrap(); //key known
            table_metadata.fields.push(row.field_name.clone());
            table_metadata.fields_set.insert(row.field_name.clone());
            table_metadata.field_counts.push(0);

            let options = DescriberOptions::builder()
                .force_string(!self.options.no_link && row.field_name.starts_with("_link"))
                .stats(self.options.stats && self.options.threads == 1)
                .build();

            table_metadata
                .describers
                .push(Describer::new_with_options(options));
            table_metadata.ignore_fields.push(false);
            match row.field_title {
                Some(field_title) => table_metadata.field_titles.push(field_title),
                None => table_metadata.field_titles.push(row.field_name),
            }

            match row.field_type {
                Some(field_type) => {
                    table_metadata.supplied_types.push(field_type.clone());
                    match field_type.as_str() {
                        "boolean" => table_metadata
                            .arrow_vecs
                            .push(ArrayBuilders::Boolean(vec![])),
                        "number" => table_metadata
                            .arrow_vecs
                            .push(ArrayBuilders::Number(vec![])),
                        "integer" => table_metadata
                            .arrow_vecs
                            .push(ArrayBuilders::Integer(vec![])),
                        _ => table_metadata
                            .arrow_vecs
                            .push(ArrayBuilders::String(vec![])),
                    }
                }
                None => {
                    table_metadata.supplied_types.push("string".into());
                    table_metadata
                        .arrow_vecs
                        .push(ArrayBuilders::String(vec![]))
                }
            }
        }

        Ok(())
    }

    pub fn mark_ignore(&mut self) {
        let one_to_many_table_names = self
            .one_to_many_arrays
            .iter()
            .map(|item| item.join(&self.options.path_separator))
            .collect_vec();

        for metadata in self.table_metadata.values_mut() {
            for (num, field) in metadata.fields.iter().enumerate() {
                let full_path = format!("{}{}", metadata.table_name_with_separator, field);
                for one_to_many_table_name in &one_to_many_table_names {
                    if full_path.starts_with(one_to_many_table_name)
                        && !metadata
                            .table_name_with_separator
                            .starts_with(one_to_many_table_name)
                    {
                        metadata.ignore_fields[num] = true;
                    }
                }
            }
        }

        for table_path in &self.one_to_one_arrays {
            let table_name = format!(
                "{}{}",
                self.options.table_prefix,
                table_path.iter().join(&self.options.path_separator)
            );
            if let Some(table_metadata) = self.table_metadata.get_mut(&table_name) {
                table_metadata.ignore = true
            }
        }
    }

    pub fn make_lower_case_titles(&mut self) {
        for metadata in self.table_metadata.values_mut() {
            let mut existing_fields = Set::new();
            for field in metadata.field_titles.iter() {
                let mut lowered = field.to_lowercase().trim().to_owned();
                lowered = INVALID_REGEX.replace_all(&lowered, "").to_string();
                while existing_fields.contains(&lowered) {
                    lowered.push('_')
                }
                metadata.field_titles_lc.push(lowered.clone());
                existing_fields.insert(lowered);
            }
        }
    }

    pub fn determine_order(&mut self) {
        for metadata in self.table_metadata.values_mut() {
            let mut fields_to_order: Vec<(usize, usize)> = vec![];

            for (num, field) in metadata.fields.iter().enumerate() {
                let full_path = format!("{}{}", metadata.table_name_with_separator, field);

                let schema_order: usize;

                if field.starts_with("_link") {
                    schema_order = 0
                } else if let Some(order) = self.order_map.get(&full_path) {
                    schema_order = *order
                } else {
                    schema_order = usize::MAX;
                }
                fields_to_order.push((schema_order, num))
            }

            fields_to_order.sort_unstable();

            for (_, field_order) in fields_to_order.iter() {
                metadata.order.push(*field_order);
            }
        }
    }

    fn log_info(&self, msg: &str) {
        info!("{}{}", self.options.thread_name, msg)
    }

    pub fn write_files(&mut self) -> Result<()> {
        self.mark_ignore();
        self.determine_order();
        self.make_lower_case_titles();

        //remove tables that should not be there from table order.
        self.table_order
            .retain(|key, _| self.table_metadata.contains_key(key));

        if self.options.memory {
            for (file, tmp_csv) in self.tmp_csvs.drain(..) {
                if let TmpCSVWriter::Memory(writer) = tmp_csv {
                    let gz_writer = writer.into_inner().unwrap(); // ok as flushing will always work as using memory
                    let csv_data = gz_writer.finish().unwrap();
                    self.tmp_memory.insert(file.clone(), csv_data);
                }
            }
        } else {
            #[cfg(not(target_family = "wasm"))]
            for (file, tmp_csv) in self.tmp_csvs.iter_mut() {
                if let TmpCSVWriter::Disk(tmp_csv) = tmp_csv {
                    tmp_csv.flush().context(FlattererFileWriteSnafu {
                        filename: file.clone(),
                    })?;
                }
            }
        }

        self.write_data_package(false)?;

        if self.options.csv
            || self.options.parquet
            || !self.options.postgres_connection.is_empty()
            || self.options.sqlite
            || !self.options.sqlite_path.is_empty()
        {
            if self.options.memory {
                self.write_csvs_memory()?;
            } else {
                self.write_csvs()?;
            }
        };

        #[cfg(not(target_family = "wasm"))]
        if self.options.sql_scripts && !self.options.memory {
            self.write_postgresql()?;
            self.write_sqlite()?;
        }

        if self.options.xlsx {
            if self.options.memory {
                #[cfg(target_family = "wasm")]
                self.write_xlsx_memory()?;
            } else {
                #[cfg(not(target_family = "wasm"))]
                self.write_xlsx()?;
            }
        };

        #[cfg(not(target_family = "wasm"))]
        if (self.options.sqlite || !self.options.sqlite_path.is_empty()) && !self.options.memory {
            self.log_info("Loading data into sqlite");

            self.write_data_package(true)?;

            let mut sqlite_dir_path = self.output_dir.join("sqlite.db");
            if !self.options.sqlite_path.is_empty() {
                sqlite_dir_path = PathBuf::from(&self.options.sqlite_path);
            }

            let options = csvs_convert::Options::builder()
                .drop(self.options.drop)
                .evolve(self.options.evolve)
                .build();

            datapackage_to_sqlite_with_options(
                sqlite_dir_path.to_string_lossy().into(),
                self.output_dir.to_string_lossy().into(),
                options,
            )
            .context(DatapackageConvertSnafu {})?;

            self.write_data_package(false)?;
        };

        #[cfg(not(target_family = "wasm"))]
        if self.options.parquet && !self.options.memory {
            self.log_info("Converting to parquet");
            let options = csvs_convert::Options::builder().build();
            datapackage_to_parquet_with_options(
                self.output_dir.join("parquet"),
                self.output_dir.to_string_lossy().into(),
                options,
            )
            .context(DatapackageConvertSnafu {})?;
        };

        #[cfg(not(target_family = "wasm"))]
        if !self.options.postgres_connection.is_empty() && !self.options.memory {
            self.log_info("Loading data into postgres");
            let options = csvs_convert::Options::builder()
                .drop(self.options.drop)
                .evolve(self.options.evolve)
                .schema(self.options.postgres_schema.clone())
                .build();
            datapackage_to_postgres_with_options(
                self.options.postgres_connection.clone(),
                self.output_dir.to_string_lossy().into(),
                options,
            )
            .context(DatapackageConvertSnafu {})?;
        };

        if self.options.memory {
            let (tables_csv, fields_csv) = write_metadata_csvs_memory_datapackage(
                self.files_memory["datapackage.json"].clone(),
            )?;
            self.files_memory.insert("tables.csv".into(), tables_csv);
            self.files_memory.insert("fields.csv".into(), fields_csv);
        } else {
            let tmp_path = self.output_dir.join("tmp");

            if remove_dir_all(&tmp_path).is_err() {
                log::warn!("Temp files can not be deleted, continuing anyway");
            }
            self.log_info("Writing metadata files");

            write_metadata_csvs_from_datapackage(self.output_dir.clone())?;

            let csv_path = self.output_dir.join("csv");
            if !self.options.csv && csv_path.is_dir() {
                remove_dir_all(&csv_path).context(FlattererRemoveDirSnafu {
                    filename: csv_path.to_string_lossy(),
                })?;
            }
        }

        Ok(())
    }

    #[cfg(not(target_family = "wasm"))]
    pub async fn write_files_async(&mut self) -> Result<()> {
        self.mark_ignore();
        self.determine_order();
        self.make_lower_case_titles();

        //remove tables that should not be there from table order.
        self.table_order
            .retain(|key, _| self.table_metadata.contains_key(key));

        for (file, tmp_csv) in self.tmp_csvs.drain(..) {
            match tmp_csv {
                TmpCSVWriter::AsyncCSV(mut tmp_csv) => {
                    tmp_csv.flush().await.context(FlattererFileWriteSnafu {
                        filename: file.clone(),
                    })?;
                    let mut inner =
                        tmp_csv
                            .into_inner()
                            .await
                            .context(FlattererFileWriteSnafu {
                                filename: file.clone(),
                            })?;

                    inner.flush().await.context(FlattererFileWriteSnafu {
                        filename: file.clone(),
                    })?;
                    inner.shutdown().await.context(FlattererFileWriteSnafu {
                        filename: file.clone(),
                    })?;
                }
                TmpCSVWriter::AsyncParquet(parquet_writer) => {
                    parquet_writer.close().await.context(ParquetSnafu {})?;
                }
                _ => {}
            }
        }

        self.write_data_package(false)?;

        self.object_store
            .as_ref()
            .expect("object store should exist")
            .put(
                &format!("{}/datapackage.json", self.output_dir.to_string_lossy()).into(),
                self.files_memory["datapackage.json"].clone().into(),
            )
            .await
            .context(ObjectStoreSnafu {})?;

        if self.options.csv {
            self.write_csvs_async().await?;
        };

        for (table_name, _) in self.table_order.iter() {
            let metadata = self.table_metadata.get(table_name).unwrap(); //key known
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let reader_path = format!(
                "{}/tmp/{}.csv",
                self.output_dir.to_string_lossy(),
                table_name
            );
            self.object_store
                .as_ref()
                .expect("object store should exist")
                .delete(&reader_path.into())
                .await
                .context(ObjectStoreSnafu {})?;
        }

        let (tables_csv, fields_csv) =
            write_metadata_csvs_memory_datapackage(self.files_memory["datapackage.json"].clone())?;

        self.object_store
            .as_ref()
            .expect("object store should exist")
            .put(
                &format!("{}/tables.csv", self.output_dir.to_string_lossy()).into(),
                tables_csv.clone().into(),
            )
            .await
            .context(ObjectStoreSnafu {})?;

        self.object_store
            .as_ref()
            .expect("object store should exist")
            .put(
                &format!("{}/fields.csv", self.output_dir.to_string_lossy()).into(),
                fields_csv.clone().into(),
            )
            .await
            .context(ObjectStoreSnafu {})?;

        Ok(())
    }

    pub fn write_data_package(&mut self, lowercase_names: bool) -> Result<()> {
        let mut resources = vec![];

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self
                .table_metadata
                .get_mut(table_name)
                .expect("table should be in metadata");
            let mut fields = vec![];
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let table_order = metadata.order.clone();
            let mut foreign_keys = vec![];

            for order in table_order {
                if metadata.ignore_fields[order] {
                    continue;
                }
                let (data_type, format) = metadata
                    .describers
                    .get_mut(order)
                    .expect("should be there")
                    .guess_type();
                let field_name = if lowercase_names {
                    &metadata.field_titles_lc[order]
                } else {
                    &metadata.fields[order]
                };
                let field_title = &metadata.field_titles[order];

                let mut field = json!({
                    "name": field_name,
                    "title": field_title,
                    "type": data_type,
                    "format": format,
                    "count": metadata.field_counts[order],
                });

                if self.options.stats && self.options.threads == 1 {
                    field
                        .as_object_mut()
                        .unwrap()
                        .insert("stats".into(), metadata.describers[order].stats());
                }

                fields.push(field);

                if field_name.starts_with("_link") && field_name != "_link" {
                    let foreign_table = self
                        .table_order
                        .get(&field_title[6..])
                        .expect("table should be in table order");
                    foreign_keys.push(
                        json!(
                        {"fields":field_title, "reference": {"resource": foreign_table, "fields": "_link"}}
                    ))
                };
            }

            let mut resource = json!({
                "profile": "tabular-data-resource",
                "name": table_name.to_lowercase(),
                "flatterer_name": table_name,
                "title": table_title,
                "schema": {
                    "fields": fields,
                    "primaryKey": "_link",
                }
            });

            if self.options.no_link {
                resource["schema"]
                    .as_object_mut()
                    .expect("just made above")
                    .remove("primaryKey");
            }

            if !foreign_keys.is_empty() {
                resource["schema"]
                    .as_object_mut()
                    .unwrap()
                    .insert("foreignKeys".into(), foreign_keys.into());
            }

            resource.as_object_mut().unwrap().insert(
                "path".to_string(),
                Value::String(format!("csv/{}.csv", table_title)),
            );

            resources.push(resource)
        }

        let data_package = json!({
            "profile": "tabular-data-package",
            "resources": resources
        });

        if self.options.memory {
            let mut datapackage = vec![];
            serde_json::to_writer_pretty(&mut datapackage, &data_package)
                .context(SerdeWriteSnafu { filename: "" })?;
            self.files_memory
                .insert("datapackage.json".into(), datapackage);
        } else {
            let metadata_file = File::create(self.output_dir.join("datapackage.json")).context(
                FlattererFileWriteSnafu {
                    filename: "datapackage.json",
                },
            )?;
            serde_json::to_writer_pretty(metadata_file, &data_package).context(
                SerdeWriteSnafu {
                    filename: "datapackage.json",
                },
            )?;
        }

        Ok(())
    }

    pub fn write_csvs_memory(&mut self) -> Result<()> {
        self.log_info("Writing final CSV files");

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self.table_metadata.get(table_name).unwrap(); //key known
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }

            let row_count = if self.options.preview == 0 {
                metadata.rows
            } else {
                std::cmp::min(self.options.preview, metadata.rows)
            };
            self.log_info(&format!(
                "    Writing {} row(s) to {}.csv",
                row_count, table_title
            ));

            let csv_gz_data = self.tmp_memory.get_mut(table_name).unwrap();

            let reader = flate2::read::GzDecoder::new(csv_gz_data.as_slice());

            let csv_reader = ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_reader(reader);

            if TERMINATE.load(Ordering::SeqCst) {
                return Err(Error::Terminated {});
            };

            let encoder = GzEncoder::new(vec![], Compression::default());
            let mut csv_writer = WriterBuilder::new().from_writer(encoder);

            let mut non_ignored_fields = vec![];

            let table_order = metadata.order.clone();

            for order in table_order {
                if !metadata.ignore_fields[order] {
                    let field = metadata.field_titles[order].clone();
                    let clean_field = INVALID_REGEX.replace_all(&field, " ");
                    non_ignored_fields.push(clean_field.to_string())
                }
            }

            csv_writer
                .write_record(&non_ignored_fields)
                .context(FlattererCSVWriteSnafu {
                    filepath: "".to_string(),
                })?;

            let mut output_row = ByteRecord::new();

            for (num, row) in csv_reader.into_byte_records().enumerate() {
                if self.options.preview != 0 && num == self.options.preview {
                    break;
                }
                let this_row = row.context(FlattererCSVWriteSnafu {
                    filepath: "".to_string(),
                })?;
                let table_order = metadata.order.clone();

                for order in table_order {
                    if metadata.ignore_fields[order] {
                        continue;
                    }
                    if order >= this_row.len() {
                        output_row.push_field(b"");
                    } else {
                        output_row.push_field(&this_row[order]);
                    }
                }

                csv_writer
                    .write_byte_record(&output_row)
                    .context(FlattererCSVWriteSnafu {
                        filepath: "".to_string(),
                    })?;
                output_row.clear();
            }

            self.csv_memory_gz.insert(
                format!("{}.csv", table_title),
                csv_writer
                    .into_inner()
                    .expect("write to csv should be safe as memory")
                    .finish()
                    .expect("write to csv should be safe as memory"),
            );

            if !self.options.xlsx {
                self.tmp_memory.remove(table_name);
            }
        }

        Ok(())
    }

    pub fn write_csvs(&mut self) -> Result<()> {
        if self.direct {
            return Ok(());
        }
        self.log_info("Writing final CSV files");
        let tmp_path = self.output_dir.join("tmp");
        let csv_path = self.output_dir.join("csv");

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self.table_metadata.get(table_name).unwrap(); //key known
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }

            let reader_filepath = tmp_path.join(format!("{}.csv", table_name));
            let csv_reader = ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_path(&reader_filepath)
                .context(FlattererCSVReadSnafu {
                    filepath: reader_filepath.to_string_lossy(),
                })?;

            let filepath = csv_path.join(format!("{}.csv", table_title));
            let row_count = if self.options.preview == 0 {
                metadata.rows
            } else {
                std::cmp::min(self.options.preview, metadata.rows)
            };
            self.log_info(&format!(
                "    Writing {} row(s) to {}.csv",
                row_count, table_title
            ));
            if TERMINATE.load(Ordering::SeqCst) {
                return Err(Error::Terminated {});
            };
            let mut csv_writer = WriterBuilder::new().from_writer(get_writer_from_path(&filepath)?);
            let mut non_ignored_fields = vec![];

            let table_order = metadata.order.clone();

            for order in table_order {
                if !metadata.ignore_fields[order] {
                    let field = metadata.field_titles[order].clone();
                    let clean_field = INVALID_REGEX.replace_all(&field, " ");
                    non_ignored_fields.push(clean_field.to_string())
                }
            }

            csv_writer
                .write_record(&non_ignored_fields)
                .context(FlattererCSVWriteSnafu {
                    filepath: filepath.clone(),
                })?;

            let mut output_row = ByteRecord::new();

            for (num, row) in csv_reader.into_byte_records().enumerate() {
                if self.options.preview != 0 && num == self.options.preview {
                    break;
                }
                let this_row = row.context(FlattererCSVWriteSnafu {
                    filepath: &filepath,
                })?;
                let table_order = metadata.order.clone();

                for order in table_order {
                    if metadata.ignore_fields[order] {
                        continue;
                    }
                    if order >= this_row.len() {
                        output_row.push_field(b"");
                    } else {
                        output_row.push_field(&this_row[order]);
                    }
                }

                csv_writer
                    .write_byte_record(&output_row)
                    .context(FlattererCSVWriteSnafu {
                        filepath: &filepath,
                    })?;
                output_row.clear();
            }
        }

        Ok(())
    }

    pub async fn write_csvs_async(&mut self) -> Result<()> {
        if self.direct {
            return Ok(());
        }
        self.log_info("Writing final CSV files");

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self.table_metadata.get(table_name).unwrap(); //key known
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }

            let reader_path = format!(
                "{}/tmp/{}.csv",
                self.output_dir.to_string_lossy(),
                table_name
            );

            let reader = self
                .object_store
                .as_ref()
                .expect("object store should exist")
                .get(&object_store::path::Path::from(reader_path))
                .await
                .context(ObjectStoreSnafu {})?;

            let stream = reader.into_stream();
            let reader = tokio_util::io::StreamReader::new(stream);

            let gzip_reader = AsycBufReader::new(reader);

            let mut csv_reader = AsyncReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .create_reader(gzip_reader);

            let filepath = format!(
                "{}/csv/{}.csv",
                self.output_dir.to_string_lossy(),
                table_name
            );

            let row_count = if self.options.preview == 0 {
                metadata.rows
            } else {
                std::cmp::min(self.options.preview, metadata.rows)
            };
            self.log_info(&format!(
                "    Writing {} row(s) to {}.csv",
                row_count, table_title
            ));
            if TERMINATE.load(Ordering::SeqCst) {
                return Err(Error::Terminated {});
            };

            let path = Path::from(filepath.clone());

            let (_id, writer) = self
                .object_store
                .as_ref()
                .expect("object store should exist")
                .put_multipart(&path)
                .await
                .context(ObjectStoreSnafu {})?;
            let mut csv_writer = AsyncWriterBuilder::new().create_writer(writer);

            let mut non_ignored_fields = vec![];

            let table_order = metadata.order.clone();

            for order in table_order {
                if !metadata.ignore_fields[order] {
                    let field = metadata.field_titles[order].clone();
                    let clean_field = INVALID_REGEX.replace_all(&field, " ");
                    non_ignored_fields.push(clean_field.to_string())
                }
            }

            csv_writer.write_record(&non_ignored_fields).await.context(
                FlattererCSVAsyncWriteSnafu {
                    filepath: filepath.clone(),
                },
            )?;

            let mut output_row = csv_async::ByteRecord::new();

            let mut stream = csv_reader.byte_records();

            use futures::stream::StreamExt;

            let mut num = 0;

            while let Some(row) = stream.next().await {
                if self.options.preview != 0 && num == self.options.preview {
                    break;
                }
                let this_row = row.context(FlattererCSVAsyncWriteSnafu {
                    filepath: &filepath,
                })?;

                let table_order = metadata.order.clone();

                for order in table_order {
                    if metadata.ignore_fields[order] {
                        continue;
                    }
                    if order >= this_row.len() {
                        output_row.push_field(b"");
                    } else {
                        output_row.push_field(&this_row[order]);
                    }
                }
                num += 1;

                csv_writer.write_byte_record(&output_row).await.context(
                    FlattererCSVAsyncWriteSnafu {
                        filepath: &filepath,
                    },
                )?;
                output_row.clear();
            }

            csv_writer.flush().await.context(FlattererFileWriteSnafu {
                filename: table_name,
            })?;
            let mut inner = csv_writer
                .into_inner()
                .await
                .context(FlattererFileWriteSnafu {
                    filename: table_name,
                })?;
            inner.flush().await.context(FlattererFileWriteSnafu {
                filename: table_name,
            })?;
            inner.shutdown().await.context(FlattererFileWriteSnafu {
                filename: table_name,
            })?;
        }

        Ok(())
    }

    #[cfg(target_family = "wasm")]
    pub fn write_xlsx_memory(&mut self) -> Result<()> {
        self.log_info("Writing final XLSX");

        let mut spreadsheet = umya_spreadsheet::new_file_empty_worksheet();

        for (_table_name, metadata) in self.table_metadata.iter() {
            if metadata.rows > 100000 || metadata.fields.len() > 65536 {
                return Ok(());
            }
        }

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self
                .table_metadata
                .get(table_name)
                .expect("table name should exist"); //key known
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let mut new_table_title = table_title.clone();

            if table_name != table_title {
                new_table_title.truncate(31);
            } else {
                new_table_title =
                    truncate_xlsx_title(new_table_title, &self.options.path_separator);
            }

            let row_count = if self.options.preview == 0 {
                metadata.rows
            } else {
                std::cmp::min(self.options.preview, metadata.rows)
            };
            self.log_info(&format!(
                "    Writing {} row(s) to sheet `{}`",
                row_count, new_table_title
            ));

            let worksheet = spreadsheet
                .new_sheet(new_table_title)
                .expect("spreadheet in memory");

            let csv_gz_data = self.tmp_memory.get_mut(table_name).unwrap();

            let reader = flate2::read::GzDecoder::new(csv_gz_data.as_slice());

            let csv_reader = ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_reader(reader);

            let mut col_index = 1;

            let table_order = metadata.order.clone();

            for order in table_order {
                if !metadata.ignore_fields[order] {
                    let mut title = metadata.field_titles[order].clone();
                    if INVALID_REGEX.is_match(&title) {
                        warn!("Characters found in input JSON that are not allowed in XLSX file or could cause issues. Striping these, so output is possible.");
                        title = INVALID_REGEX.replace_all(&title, "").to_string();
                    }

                    worksheet
                        .get_cell_by_column_and_row_mut(&col_index, &1)
                        .set_value(title);
                    col_index += 1;
                }
            }

            for (row_num, row) in csv_reader.into_records().enumerate() {
                if self.options.preview != 0 && row_num == self.options.preview {
                    break;
                }
                col_index = 1;
                let this_row = row.context(FlattererCSVReadSnafu {
                    filepath: table_name,
                })?;

                let table_order = metadata.order.clone();

                for order in table_order {
                    if metadata.ignore_fields[order] {
                        continue;
                    }
                    if order >= this_row.len() {
                        continue;
                    }

                    let mut cell = this_row[order].to_string();

                    if INVALID_REGEX.is_match(&cell) {
                        warn!("Character found in JSON that is not allowed in XLSX file. Removing these so output is possible");
                        cell = INVALID_REGEX.replace_all(&cell, "").to_string();
                    }

                    worksheet
                        .get_cell_by_column_and_row_mut(
                            &col_index,
                            &(row_num + 2).try_into().expect("row columns shoud exist"),
                        )
                        .set_value(cell);
                    // if metadata.field_type[order] == "number" {
                    //     if let Ok(number) = cell.parse::<f64>() {
                    //         worksheet.get_cell_by_column_and_row_mut(&(row_num + 1).try_into().unwrap(), &col_index).set_value(number.into());

                    //     } else {
                    //         worksheet.get_cell_by_column_and_row_mut(&(row_num + 1).try_into().unwrap(), &col_index).set_value(cell);
                    //     };
                    // } else {
                    //     worksheet.get_cell_by_column_and_row_mut(&(row_num + 1).try_into().unwrap(), &col_index).set_value(cell);
                    // }
                    col_index += 1
                }
            }

            self.tmp_memory.remove(table_name);
        }

        let mut output = std::io::Cursor::new(vec![]);
        {
            umya_spreadsheet::writer::xlsx::write_writer(&spreadsheet, &mut output)
                .expect("xlsx writing should work as its to memory");
        }

        self.files_memory
            .insert("output.xlsx".into(), output.into_inner());

        return Ok(());
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn write_xlsx(&mut self) -> Result<()> {
        self.log_info("Writing final XLSX file");
        let csv_path = self
            .output_dir
            .join(if self.direct { "csv" } else { "tmp" });

        let workbook = Workbook::new_with_options(
            &self.output_dir.join("output.xlsx").to_string_lossy(),
            true,
            Some(&csv_path.to_string_lossy()),
            false,
        ).context(FlattererXLSXSnafu)?;

        for (table_name, metadata) in self.table_metadata.iter() {
            if metadata.rows > 1048576 {
                return Err(Error::XLSXTooManyRows {
                    rows: metadata.rows,
                    sheet: table_name.clone(),
                });
            }
            if metadata.fields.len() > 65536 {
                return Err(Error::XLSXTooManyColumns {
                    columns: metadata.fields.len(),
                    sheet: table_name.clone(),
                });
            }
        }

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self.table_metadata.get(table_name).unwrap(); //key known
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let mut new_table_title = table_title.clone();

            if table_name != table_title {
                new_table_title.truncate(31);
            } else {
                new_table_title =
                    truncate_xlsx_title(new_table_title, &self.options.path_separator);
            }
            if TERMINATE.load(Ordering::SeqCst) {
                return Err(Error::Terminated {});
            };
            let row_count = if self.options.preview == 0 {
                metadata.rows
            } else {
                std::cmp::min(self.options.preview, metadata.rows)
            };
            self.log_info(&format!(
                "    Writing {} row(s) to sheet `{}`",
                &row_count, &new_table_title
            ));

            let metadata = self.table_metadata.get_mut(table_name).unwrap(); //key known

            let mut worksheet = workbook
                .add_worksheet(Some(&new_table_title))
                .context(FlattererXLSXSnafu {})?;

            let filepath = csv_path.join(format!("{}.csv", table_name));
            let csv_reader = ReaderBuilder::new()
                .has_headers(self.direct)
                .flexible(!self.direct)
                .from_path(filepath.clone())
                .context(FlattererCSVReadSnafu {
                    filepath: filepath.to_string_lossy(),
                })?;

            let mut col_index = 0;

            let table_order = metadata.order.clone();

            for order in table_order {
                if !metadata.ignore_fields[order] {
                    let mut title = metadata.field_titles[order].clone();
                    if INVALID_REGEX.is_match(&title) {
                        warn!("Characters found in input JSON that are not allowed in XLSX file or could cause issues. Striping these, so output is possible.");
                        title = INVALID_REGEX.replace_all(&title, "").to_string();
                    }

                    worksheet
                        .write_string(0, col_index, &title, None)
                        .context(FlattererXLSXSnafu {})?;
                    col_index += 1;
                }
            }

            for (row_num, row) in csv_reader.into_records().enumerate() {
                if self.options.preview != 0 && row_num == self.options.preview {
                    break;
                }
                col_index = 0;
                let this_row = row.context(FlattererCSVReadSnafu {
                    filepath: filepath.to_string_lossy(),
                })?;

                let table_order = metadata.order.clone();

                for order in table_order {
                    if metadata.ignore_fields[order] {
                        continue;
                    }
                    if order >= this_row.len() {
                        continue;
                    }

                    let mut cell = this_row[order].to_string();

                    if INVALID_REGEX.is_match(&cell) {
                        warn!("Character found in JSON that is not allowed in XLSX file. Removing these so output is possible");
                        cell = INVALID_REGEX.replace_all(&cell, "").to_string();
                    }

                    if cell.len() > 32767 {
                        log::warn!("WARNING: Cell larger than 32767 chararcters which is too large for XLSX format. The cell will be truncated, so some data will be missing.");
                        cell.truncate(32767)
                    }

                    if metadata.describers[order].guess_type().0 == "number" {
                        if let Ok(number) = cell.parse::<f64>() {
                            worksheet
                                .write_number(
                                    (row_num + 1).try_into().context(FlattererIntSnafu {})?,
                                    col_index,
                                    number,
                                    None,
                                )
                                .context(FlattererXLSXSnafu {})?;
                        } else {
                            worksheet
                                .write_string(
                                    (row_num + 1).try_into().context(FlattererIntSnafu {})?,
                                    col_index,
                                    &cell,
                                    None,
                                )
                                .context(FlattererXLSXSnafu {})?;
                        };
                    } else {
                        worksheet
                            .write_string(
                                (row_num + 1).try_into().context(FlattererIntSnafu {})?,
                                col_index,
                                &cell,
                                None,
                            )
                            .context(FlattererXLSXSnafu {})?;
                    }
                    col_index += 1
                }
            }
        }
        workbook.close().context(FlattererXLSXSnafu {})?;

        Ok(())
    }

    pub fn write_postgresql(&mut self) -> Result<()> {
        let postgresql_dir_path = self.output_dir.join("postgresql");
        create_dir_all(&postgresql_dir_path).context(FlattererCreateDirSnafu {
            filename: postgresql_dir_path.to_string_lossy(),
        })?;

        let mut postgresql_schema = File::create(postgresql_dir_path.join("postgresql_schema.sql"))
            .context(FlattererFileWriteSnafu {
                filename: "postgresql_schema.sql",
            })?;
        let mut postgresql_load = File::create(postgresql_dir_path.join("postgresql_load.sql"))
            .context(FlattererFileWriteSnafu {
                filename: "postgresql_load.sql",
            })?;

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self.table_metadata.get_mut(table_name).unwrap();
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let table_order = metadata.order.clone();
            writeln!(
                postgresql_schema,
                "CREATE TABLE \"{ }\"(",
                table_title.to_lowercase()
            )
            .context(FlattererFileWriteSnafu {
                filename: "postgresql_schema.sql",
            })?;

            let mut fields = Vec::new();
            for order in table_order {
                if metadata.ignore_fields[order] {
                    continue;
                }
                fields.push(format!(
                    "    \"{}\" {}",
                    metadata.field_titles_lc[order],
                    postgresql::to_postgresql_type(&metadata.describers[order].guess_type().0)
                ));
            }
            write!(postgresql_schema, "{}", fields.join(",\n")).context(
                FlattererFileWriteSnafu {
                    filename: "postgresql_schema.sql",
                },
            )?;
            write!(postgresql_schema, ");\n\n").context(FlattererFileWriteSnafu {
                filename: "postgresql_schema.sql",
            })?;

            writeln!(
                postgresql_load,
                "\\copy \"{}\" from 'csv/{}.csv' with CSV HEADER",
                table_title.to_lowercase(),
                table_title,
            )
            .context(FlattererFileWriteSnafu {
                filename: "postgresql_load.sql",
            })?;
        }

        Ok(())
    }

    pub fn write_sqlite(&mut self) -> Result<()> {
        let sqlite_dir_path = self.output_dir.join("sqlite");
        create_dir_all(&sqlite_dir_path).context(FlattererCreateDirSnafu {
            filename: sqlite_dir_path.to_string_lossy(),
        })?;

        let mut sqlite_schema = File::create(sqlite_dir_path.join("sqlite_schema.sql")).context(
            FlattererFileWriteSnafu {
                filename: "sqlite_schema.sql",
            },
        )?;
        let mut sqlite_load = File::create(sqlite_dir_path.join("sqlite_load.sql")).context(
            FlattererFileWriteSnafu {
                filename: "sqlite_schema.sql",
            },
        )?;

        writeln!(sqlite_load, ".mode csv ").context(FlattererFileWriteSnafu {
            filename: "sqlite_schema.sql",
        })?;

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self.table_metadata.get_mut(table_name).unwrap();
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let table_order = metadata.order.clone();
            writeln!(
                sqlite_schema,
                "CREATE TABLE \"{ }\"(",
                table_title.to_lowercase()
            )
            .context(FlattererFileWriteSnafu {
                filename: "sqlite_schema.sql",
            })?;

            let mut fields = Vec::new();
            for order in table_order {
                if metadata.ignore_fields[order] {
                    continue;
                }
                fields.push(format!(
                    "    \"{}\" {}",
                    metadata.field_titles_lc[order],
                    postgresql::to_postgresql_type(&metadata.describers[order].guess_type().0)
                ));
            }
            write!(sqlite_schema, "{}", fields.join(",\n")).context(FlattererFileWriteSnafu {
                filename: "sqlite_schema.sql",
            })?;
            write!(sqlite_schema, ");\n\n").context(FlattererFileWriteSnafu {
                filename: "sqlite_schema.sql",
            })?;

            writeln!(
                sqlite_load,
                ".import 'csv/{}.csv' {} --skip 1 ",
                table_title,
                table_title.to_lowercase()
            )
            .context(FlattererFileWriteSnafu {
                filename: "sqlite_load.sql",
            })?;
        }

        Ok(())
    }
}

fn value_convert(value: Value, describers: &mut Vec<Describer>, num: usize) -> String {
    let describer = &mut describers[num];

    match value {
        Value::String(val) => {
            describer.process(&val);
            val
        }
        Value::Null => {
            describer.process("");
            "".to_string()
        }
        Value::Number(number) => {
            describer.process_num(number.as_f64().unwrap_or(0_f64));
            number.to_string()
        }
        Value::Bool(bool) => {
            let string = bool.to_string();
            describer.process(&string);
            string
        }
        Value::Array(_) => {
            let string = value.to_string();
            describer.process(&string);
            string
        }
        Value::Object(_) => {
            let string = value.to_string();
            describer.process(&string);
            string
        }
    }
}

pub fn truncate_xlsx_title(mut title: String, seperator: &str) -> String {
    let parts: Vec<&str> = title.split(seperator).collect();
    if parts.len() == 1 || title.len() <= 31 {
        title.truncate(31);
        return title;
    }

    let mut last_part = parts.last().unwrap().to_string();

    let length_of_last_part = parts.last().unwrap().len();

    let rest = 31 - std::cmp::min(length_of_last_part, 31);

    let max_len_of_part_with_sep = rest / (parts.len() - 1);

    let len_of_part =
        max_len_of_part_with_sep - std::cmp::min(max_len_of_part_with_sep, seperator.len());

    if len_of_part < 1 {
        last_part.truncate(31);
        return last_part;
    }
    let mut new_parts: Vec<String> = vec![];
    for part in parts[..parts.len() - 1].iter() {
        let end_new_part = std::cmp::min(len_of_part, part.len());
        let new_part = part[..end_new_part].to_string();
        new_parts.push(new_part);
    }
    new_parts.push(last_part);

    new_parts.join(seperator)
}

pub fn write_metadata_csvs_memory_datapackage(datapackage: Vec<u8>) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut fields_writer = Writer::from_writer(vec![]);
    let mut tables_writer = Writer::from_writer(vec![]);

    let json: Value = serde_json::from_reader(datapackage.as_slice()).context(SerdeReadSnafu {})?;

    let resources = json["resources"].as_array().unwrap();

    fields_writer
        .write_record([
            "table_name",
            "field_name",
            "field_type",
            "field_title",
            "count",
        ])
        .context(FlattererCSVWriteSnafu {
            filepath: PathBuf::new(),
        })?;

    tables_writer
        .write_record(["table_name", "table_title"])
        .context(FlattererCSVWriteSnafu {
            filepath: PathBuf::new(),
        })?;

    for resource in resources {
        tables_writer
            .write_record([
                resource["flatterer_name"].as_str().unwrap(),
                resource["title"].as_str().unwrap(),
            ])
            .context(FlattererCSVWriteSnafu {
                filepath: PathBuf::new(),
            })?;
        for field_value in resource["schema"]["fields"].as_array().unwrap() {
            let field_type = match field_value["type"].as_str().unwrap() {
                "string" => "text",
                "datetime" => "date",
                rest => rest,
            };

            fields_writer
                .write_record([
                    resource["flatterer_name"].as_str().unwrap(),
                    field_value["name"].as_str().unwrap(),
                    field_type,
                    field_value["title"].as_str().unwrap(),
                    &field_value["count"].as_i64().unwrap().to_string(),
                ])
                .context(FlattererCSVWriteSnafu {
                    filepath: PathBuf::new(),
                })?;
        }
    }

    Ok((
        tables_writer.into_inner().unwrap(),
        fields_writer.into_inner().unwrap(),
    ))
}

fn get_writer_from_path(path: &PathBuf) -> Result<File> {
    let writer_res = File::create(path);

    if let Err(io_error) = &writer_res {
        if let Some(code) = io_error.raw_os_error() {
            if code == 24 {
                return Err(Error::FlattererOSError {
                    message: "Could not write to file as your operating system says it has too many files open. Try and change limit by setting `ulimit -n 1024`".into()}
                );
            }
        }
    }
    writer_res.context(FlattererFileWriteSnafu {
        filename: path.to_string_lossy(),
    })
}

pub fn write_metadata_csvs_from_datapackage(output_dir: PathBuf) -> Result<()> {
    let datapackage_path = output_dir.join("datapackage.json");

    let fields_filepath = output_dir.join("fields.csv");
    let mut fields_writer = Writer::from_writer(get_writer_from_path(&fields_filepath)?);

    let tables_filepath = output_dir.join("tables.csv");
    let mut tables_writer = Writer::from_writer(get_writer_from_path(&tables_filepath)?);

    let file = File::open(&datapackage_path).context(FlattererReadSnafu {
        filepath: &datapackage_path,
    })?;
    let json: Value = serde_json::from_reader(BufReader::new(file)).context(SerdeReadSnafu {})?;

    let resources = json["resources"].as_array().unwrap();

    fields_writer
        .write_record([
            "table_name",
            "field_name",
            "field_type",
            "field_title",
            "count",
        ])
        .context(FlattererCSVWriteSnafu {
            filepath: fields_filepath.clone(),
        })?;

    tables_writer
        .write_record(["table_name", "table_title"])
        .context(FlattererCSVWriteSnafu {
            filepath: tables_filepath.clone(),
        })?;

    for resource in resources {
        tables_writer
            .write_record([
                resource["flatterer_name"].as_str().unwrap(),
                resource["title"].as_str().unwrap(),
            ])
            .context(FlattererCSVWriteSnafu {
                filepath: tables_filepath.clone(),
            })?;
        for field_value in resource["schema"]["fields"].as_array().unwrap() {
            let field_type = match field_value["type"].as_str().unwrap() {
                "string" => "text",
                "datetime" => "date",
                rest => rest,
            };

            fields_writer
                .write_record([
                    resource["flatterer_name"].as_str().unwrap(),
                    field_value["name"].as_str().unwrap(),
                    field_type,
                    field_value["title"].as_str().unwrap(),
                    &field_value["count"].as_i64().unwrap().to_string(),
                ])
                .context(FlattererCSVWriteSnafu {
                    filepath: fields_filepath.clone(),
                })?;
        }
    }

    Ok(())
}

pub fn flatten_to_memory<R: Read>(input: BufReader<R>, mut options: Options) -> Result<FlatFiles> {
    options.memory = true;

    let options_clone = options.clone();

    let mut flat_files = FlatFiles::new("".into(), options_clone.clone())?;

    let mut count = 0;

    if options.ndjson || options.json_stream {
        let stream = Deserializer::from_reader(input).into_iter::<Value>();

        for item in stream {
            let value = item.context(SerdeReadSnafu {})?;

            if !value.is_object() {
                return Err(Error::FlattererProcessError {
                    message: format!(
                        "Value at array position {} is not an object: value is `{}`",
                        count, value
                    ),
                });
            }
            flat_files.process_value(value, vec![]);
            flat_files.create_rows()?;
            count += 1;
            if count % 500000 == 0 {
                flat_files.log_info(&format!("Processed {} values so far.", count));
            }
        }
    } else {
        let mut json: Value = serde_json::from_reader(input).context(SerdeReadSnafu {})?;
        if !options.path.is_empty() {
            let joined = options.path.join("/");
            let json_pointer = format!("/{joined}");
            if let Some(pointed) = json.pointer_mut(&json_pointer) {
                json = pointed.take();
            } else {
                return Err(Error::FlattererProcessError {
                    message: format!("No value at given path"),
                });
            }
        }

        if json.is_array() {
            for value in json.as_array_mut().unwrap() {
                flat_files.process_value(value.take(), vec![]);
                flat_files.create_rows()?;
                count += 1;
                if count % 500000 == 0 {
                    flat_files.log_info(&format!("Processed {} values so far.", count));
                }
            }
        } else if json.is_object() {
            flat_files.process_value(json, vec![]);
            flat_files.create_rows()?;
        }
    }

    flat_files.write_files()?;

    Ok(flat_files)
}

#[cfg(not(target_family = "wasm"))]
pub fn flatten_all(
    inputs: Vec<String>,
    output: String,
    options: Options,
) -> Result<()> {

    let mut final_options = options.clone();

    let mut analysis_options = options.clone();
    let mut do_analysis = false;

    if output.starts_with("s3://") {
        final_options.s3 = true;
        final_options.memory = true;
        do_analysis = true;
        if options.xlsx || options.sqlite || !options.postgres_connection.is_empty() {
            return Err(Error::FlattererOptionError { message: "When writing to s3 can only choose CSV or Parquet outputs".into() })
        }
    }

    if options.low_disk {
        do_analysis = true
    }

    if (!options.fields_csv.is_empty() || !options.fields_csv_string.is_empty()) && options.only_fields {
        do_analysis = false;
    } 

    if !final_options.s3 && inputs.len() == 1 && !options.low_disk && options.threads != 1 {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        rt.block_on( async {
            let buf_read = get_buf_read(inputs[0].clone(), options.gzip_input).await?;
            let output = rt.spawn_blocking(move || {
                flatten(buf_read, output, options)
            });
            output.await.context(JoinSnafu)??;
            Ok::<(), Error>(())
        })?;
        return Ok(())
    }

    if do_analysis {
        info!("Doing analysis first");
        if inputs.contains(&"-".to_string()) {
            return Err(Error::FlattererOptionError { message: "Can not use stdin when using `low_disk` or exporting to s3 without supplying fields.csv".into() })
        }
        analysis_options.memory = true;
        analysis_options.csv = false;
        analysis_options.xlsx = false;
        analysis_options.parquet = false;
        analysis_options.sqlite = false;
        analysis_options.s3 = false;
        analysis_options.postgres_connection = "".into();

        let mut analysis_flat_files = FlatFiles::new("".into(), analysis_options.clone())?;

        for input in inputs.iter() {
            analysis_flat_files = flatten_single(input.into(), analysis_flat_files)?
        }

        analysis_flat_files.write_files()?;
        let fields_bytes = analysis_flat_files.files_memory.get("fields.csv").expect("should exist");
        final_options.fields_csv_string = String::from_utf8_lossy(&fields_bytes).to_string();
        final_options.only_fields = true;
    }


    let mut flat_files = FlatFiles::new(
        output.clone(),
        final_options.clone(),
    )?;
    let s3 = flat_files.options.s3;

    if do_analysis {
        info!("Doing final run");
    }
    for input in inputs {
        let flat_files_result = flatten_single(input, flat_files);

        if flat_files_result.is_err() {
            if !s3 {
                remove_dir_all(PathBuf::from(&output)).context(FlattererRemoveDirSnafu {
                    filename: PathBuf::from(&output).to_string_lossy(),
                })?;
            }
        }
        flat_files = flat_files_result?;
    }

    if output.starts_with("s3://") {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        rt.block_on( async {
            flat_files.write_files_async().await?;
            Ok(())
        })
    } else {
        let flat_files_result = flat_files.write_files();
        if flat_files_result.is_err() {
            remove_dir_all(PathBuf::from(&output)).context(FlattererRemoveDirSnafu {
                filename: PathBuf::from(&output).to_string_lossy(),
            })?;
        }
        Ok(())
    }
}

async fn get_buf_read(input: String, gzip: bool) -> Result<Box<dyn BufRead + Send>, Error> {
    let buf_reader: Box<dyn BufRead + Send> = if input.starts_with("http") {
        let http_builder = object_store::http::HttpBuilder::new();
        let http = http_builder.with_url(&input).build().context(ObjectStoreSnafu {})?;
        let http_response = http.get(&Path::from("")).await.context(ObjectStoreSnafu {})?;
        let stream = http_response.into_stream();
        let async_reader = tokio_util::io::StreamReader::new(stream);

        if input.ends_with(".gz") || gzip{
            let mut gzip_decoder = GzipDecoder::new(async_reader);
            gzip_decoder.multiple_members(true);
            let deflate_reader = tokio::io::BufReader::new(gzip_decoder);
            let reader = tokio_util::io::SyncIoBridge::new(deflate_reader);
            Box::new(reader)
        } else {
            let reader = tokio_util::io::SyncIoBridge::new(async_reader);
            Box::new(reader)
        }
    } else if input.starts_with("s3://") {
        let s3 = AmazonS3Builder::from_env()
            .with_url(&input)
            .build()
            .context(ObjectStoreSnafu {})?;

        let url = url::Url::parse(&input).context(URLSnafu {})?;
        let mut path = url.path().to_string();
        path.remove(0);

        let http_response = s3.get(&Path::from(path)).await.context(ObjectStoreSnafu {})?;

        let stream = http_response.into_stream();
        let async_reader = tokio_util::io::StreamReader::new(stream);

        if input.ends_with(".gz") || gzip {
            let mut gzip_decoder = GzipDecoder::new(async_reader);
            gzip_decoder.multiple_members(true);
            let deflate_reader = tokio::io::BufReader::new(gzip_decoder);
            let reader = tokio_util::io::SyncIoBridge::new(deflate_reader);
            Box::new(reader)
        } else {
            let reader = tokio_util::io::SyncIoBridge::new(async_reader);
            Box::new(reader)
        }
    } else if input == "-" {
        if gzip {
            let gz_reader = flate2::read::MultiGzDecoder::new(std::io::stdin());
            Box::new(BufReader::new(gz_reader))
        } else {
            Box::new(BufReader::new(std::io::stdin()))
        }
    } else {
        let file = File::open(&input).context(FlattererReadSnafu {
            filepath: input.clone(),
        })?;
        if input.ends_with(".gz") || gzip {
            info!("gzip reader");
            let gz_reader = flate2::read::MultiGzDecoder::new(file);
            Box::new(BufReader::new(gz_reader))
        } else {
            Box::new(BufReader::new(file))
        }
    };
    Ok(buf_reader)
}


#[cfg(not(target_family = "wasm"))]
pub fn flatten_single(
    input: String,
    flat_files: FlatFiles,
) -> Result<FlatFiles> {
    let (item_sender, item_receiver): (Sender<Item>, Receiver<yajlparser::Item>) = bounded(1000);

    let (stop_sender, stop_receiver) = bounded(1);

    let options = flat_files.options.clone();
    let options_clone = flat_files.options.clone();

    let join_handler = thread::spawn(move || { 

        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        let output = rt.block_on( async {
            let mut reciever = item_receiver;
            log::debug!("Starting item reviever");
            item_reciever(options_clone, &mut reciever, flat_files, stop_receiver).await
        });
        if let Err(err) = output.as_ref() {
            log::debug!("Error {} - {:?}", err, err);
        }
        output
    });

    let send_join_handler = thread::spawn(move || {

        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        rt.block_on( async {
            let buf_read = get_buf_read(input, options.gzip_input).await?;
            rt.spawn_blocking(move || {
                send_json_items(&options, buf_read, item_sender, vec![stop_sender], None)
            }).await.context(JoinSnafu)
        })

    });

    match send_join_handler.join() {
        Ok(result) => {
            let result = result?;
            if let Err(err) = result {
                log::debug!("Error on thread join {:?}", err);
                return Err(err);
            }
        }
        Err(err) => panic::resume_unwind(err),
    }

    match join_handler.join() {
        Ok(result) => {
            result
        }
        Err(err) => panic::resume_unwind(Box::new(err))
    }
}

async fn item_reciever(options_clone: Options, item_receiver: &mut Receiver<Item>, mut flat_files: FlatFiles, stop_receiver: Receiver<()>) -> Result<FlatFiles, Error> {
    let smart_path = options_clone
        .path
        .iter()
        .map(SmartString::from)
        .collect_vec();
    let mut count = 0;
    for item in item_receiver.iter() {
        let serde_result = serde_json::from_str(&item.json);
        if serde_result.is_err() && item.json.as_bytes().iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if !options_clone.path.is_empty() && item.path != smart_path {
            continue;
        }

        if serde_result.is_err() && !options_clone.json_stream {
            return Err(Error::FlattererProcessError {
                message: "Error parsing JSON, try the --json-stream option.".into(),
            });
        }

        let value: Value = serde_result.context(SerdeReadSnafu {})?;


        if !value.is_object() {
            return Err(Error::FlattererProcessError {
                message: format!(
                    "Value at array position {} is not an object: value is `{}`",
                    count, value
                ),
            });
        }

        count += 1;

        let mut initial_path = vec![];
        if smart_path.is_empty() {
            initial_path = item.path.clone()
        }

        flat_files.process_value(value, initial_path);
        if flat_files.options.s3 {
            if flat_files.options.parquet {
                if count % 100 == 0 {
                    flat_files.create_arrow_cols().await?;
                }
            } else { 
                match flat_files.create_rows_async().await {
                    Ok(_) => {}
                    Err(e) => {
                        for (_, writer) in flat_files.tmp_csvs.drain(..) {
                            if let TmpCSVWriter::AsyncCSV(mut writer) = writer {
                                writer.flush().await.context(FlattererIoSnafu)?;
                                drop(writer)
                            }
                        }
                        return Err(e);
                    }
                };
            }
        } else {
            flat_files.create_rows()?
        }
        if count % 500000 == 0 {
            flat_files.log_info(&format!("Processed {} values so far.", count));
            if TERMINATE.load(Ordering::SeqCst) {
                log::debug!("Terminating..");
                return Err(Error::Terminated {});
            };
            if stop_receiver.try_recv().is_ok() {
                return Ok(flat_files); // This is really an error but it should be handled by main thread.
            }
        }
    }

    if flat_files.options.s3 {
        if flat_files.options.parquet {
            flat_files.create_arrow_cols().await?;
        }
    }

    if count == 0 && flat_files.options.threads != 2 {
        return Err(Error::FlattererProcessError {
            message: "The JSON provided as input is not an array of objects".to_string(),
        });
    }
    if stop_receiver.try_recv().is_ok() {
        return Ok(flat_files); // This is really an error but it should be handled by main thread.
    }
    if TERMINATE.load(Ordering::SeqCst) {
        return Err(Error::Terminated {});
    };
    flat_files.log_info(&format!(
        "{}Finished processing {} value(s)",
        options_clone.thread_name, count
    ));
    Ok(flat_files)
}

#[cfg(not(target_family = "wasm"))]
pub fn flatten(input: Box<dyn BufRead>, output: String, mut options: Options) -> Result<()> {
    if options.threads == 0 {
        options.threads = num_cpus::get()
    }

    let final_output_path = PathBuf::from(output);
    let parts_path = final_output_path.join("parts");

    if options.threads > 1 {
        if options.inline_one_to_one {
            warn!("Inline one-to-one may not work correctly when using muliple threads");
        }
        if final_output_path.is_dir() {
            if options.force {
                remove_dir_all(&final_output_path).context(FlattererRemoveDirSnafu {
                    filename: final_output_path.to_string_lossy(),
                })?;
            } else {
                return Err(Error::FlattererDirExists {
                    dir: final_output_path,
                });
            }
        }

        create_dir_all(&parts_path).context(FlattererCreateDirSnafu {
            filename: parts_path.to_string_lossy(),
        })?;
    }

    let mut output_paths = vec![];
    let mut join_handlers = vec![];

    let (item_sender, item_receiver_initial): (Sender<Item>, Receiver<yajlparser::Item>) =
        bounded(1000);

    let mut stop_senders = vec![];

    for index in 0..options.threads {
        let (stop_sender, stop_receiver) = bounded(1);
        stop_senders.push(stop_sender);

        let mut options_clone = options.clone();

        let mut output_path = final_output_path.clone();

        if options.threads > 1 {
            options_clone.id_prefix = format!("id-{}.{}", index, options_clone.id_prefix);
            options_clone.csv = true;
            options_clone.xlsx = false;
            options_clone.sqlite = false;
            options_clone.parquet = false;
            options_clone.postgres_connection = "".into();
            options_clone.thread_name = format!("Thread {index}: ");
            output_path = parts_path.join(index.to_string());
        }
        output_paths.push(output_path.clone().to_string_lossy().to_string());

        let mut flat_files = FlatFiles::new(
            output_path.clone().to_string_lossy().to_string(),
            options_clone.clone(),
        )?;
        let item_receiver = item_receiver_initial.clone();

        let join_handler = thread::spawn(move || {
            let smart_path = options_clone
                .path
                .iter()
                .map(SmartString::from)
                .collect_vec();
            let mut count = 0;
            for item in item_receiver.iter() {
                let serde_result = serde_json::from_str(&item.json);

                if serde_result.is_err() && item.json.as_bytes().iter().all(u8::is_ascii_whitespace)
                {
                    continue;
                }
                if !options_clone.path.is_empty() && item.path != smart_path {
                    continue;
                }

                if serde_result.is_err() && !options_clone.json_stream {
                    return Err(Error::FlattererProcessError {
                        message: "Error parsing JSON, try the --json-stream option.".into(),
                    });
                }

                let value: Value = serde_result.context(SerdeReadSnafu {})?;

                if !value.is_object() {
                    return Err(Error::FlattererProcessError {
                        message: format!(
                            "Value at array position {} is not an object: value is `{}`",
                            count, value
                        ),
                    });
                }

                let mut initial_path = vec![];
                if smart_path.is_empty() {
                    initial_path = item.path.clone()
                }

                flat_files.process_value(value, initial_path);
                flat_files.create_rows()?;
                count += 1;
                if count % 500000 == 0 {
                    flat_files.log_info(&format!("Processed {} values so far.", count));
                    if TERMINATE.load(Ordering::SeqCst) {
                        log::debug!("Terminating..");
                        return Err(Error::Terminated {});
                    };
                    if stop_receiver.try_recv().is_ok() {
                        return Ok(flat_files); // This is really an error but it should be handled by main thread.
                    }
                }
            }

            if count == 0 && options.threads != 2 {
                return Err(Error::FlattererProcessError {
                    message: "The JSON provided as input is not an array of objects".to_string(),
                });
            }
            if stop_receiver.try_recv().is_ok() {
                return Ok(flat_files); // This is really an error but it should be handled by main thread.
            }
            if TERMINATE.load(Ordering::SeqCst) {
                return Err(Error::Terminated {});
            };
            flat_files.log_info(&format!(
                "{}Finished processing {} value(s)",
                options_clone.thread_name, count
            ));

            flat_files.write_files()?;
            Ok(flat_files)
        });
        join_handlers.push(join_handler);
    }
    drop(item_receiver_initial);

    send_json_items(
        &options,
        Box::new(input),
        item_sender,
        stop_senders,
        Some(&final_output_path),
    )?;

    for join_handler in join_handlers {
        match join_handler.join() {
            Ok(result) => {
                if let Err(err) = result {
                    log::debug!("Error on thread join {}", err);
                    remove_dir_all(&final_output_path).context(FlattererRemoveDirSnafu {
                        filename: final_output_path.to_string_lossy(),
                    })?;
                    return Err(err);
                }
            }
            Err(err) => panic::resume_unwind(err),
        }
    }

    #[cfg(not(target_family = "wasm"))]
    if options.threads > 1 {
        let op = csvs_convert::Options::builder()
            .delete_input_csv(true)
            .build();
        info!("Merging results");
        merge_datapackage_with_options(final_output_path.clone(), output_paths, op)
            .context(DatapackageConvertSnafu {})?;

        remove_dir_all(&parts_path).context(FlattererRemoveDirSnafu {
            filename: parts_path.to_string_lossy(),
        })?;

        if options.parquet {
            info!("Writing merged parquet files");
            let op = csvs_convert::Options::builder().build();
            datapackage_to_parquet_with_options(
                final_output_path.join("parquet"),
                final_output_path.to_string_lossy().into(),
                op,
            )
            .context(DatapackageConvertSnafu {})?;
        }

        if !options.postgres_connection.is_empty() {
            info!("Loading data into postgres");
            let op = csvs_convert::Options::builder()
                .drop(options.drop)
                .schema(options.postgres_schema.clone())
                .evolve(options.evolve)
                .build();
            datapackage_to_postgres_with_options(
                options.postgres_connection.clone(),
                final_output_path.to_string_lossy().into(),
                op,
            )
            .context(DatapackageConvertSnafu {})?;
        };

        if options.sqlite {
            info!("Writing merged sqlite file");
            let op = csvs_convert::Options::builder()
                .drop(options.drop)
                .evolve(options.evolve)
                .build();

            if options.sqlite_path.is_empty() {
                options.sqlite_path = final_output_path.join("sqlite.db").to_string_lossy().into();
            }
            datapackage_to_sqlite_with_options(
                options.sqlite_path,
                final_output_path.to_string_lossy().into(),
                op,
            )
            .context(DatapackageConvertSnafu {})?;
        }

        if options.xlsx {
            info!("Writing merged xlsx file");
            let op = csvs_convert::Options::builder().build();

            datapackage_to_xlsx_with_options(
                final_output_path
                    .join("output.xlsx")
                    .to_string_lossy()
                    .into(),
                final_output_path.to_string_lossy().into(),
                op,
            )
            .context(DatapackageConvertSnafu {})?;
        }

        if !options.csv {
            remove_dir_all(final_output_path.join("csv")).context(FlattererRemoveDirSnafu {
                filename: final_output_path.to_string_lossy(),
            })?;
        }
        write_metadata_csvs_from_datapackage(final_output_path)?;
    }

    Ok(())
}


fn send_json_items(
    options: &Options,
    mut input: Box<dyn BufRead>,
    item_sender: Sender<Item>,
    stop_senders: Vec<Sender<()>>,
    final_output_path: Option<&PathBuf>,
) -> Result<(), Error> {
    let mut outer: Vec<u8> = vec![];
    let top_level_type;
    if options.ndjson {
        for line in input.lines() {
            item_sender
                .send(Item {
                    json: line.context(FlattererReadSnafu { filepath: "input" })?,
                    path: vec![],
                })
                .context(ChannelItemSnafu {})?;
        }
    } else {
        {
            let mut handler = yajlparser::ParseJson::new(
                &mut outer,
                Some(item_sender.clone()),
                None,
                options.json_stream,
                500 * 1024 * 1024,
            );

            {
                let mut parser = Parser::new(&mut handler);

                if let Err(error) = parser.parse(&mut input) {
                    log::debug!("Parse error, whilst in main parsing");
                    for stop_sender in stop_senders {
                        stop_sender.send(()).context(ChannelStopSendSnafu {})?;
                    }
                    if let Some(final_output_path) = final_output_path {
                        remove_dir_all(final_output_path).context(FlattererRemoveDirSnafu {
                            filename: final_output_path.to_string_lossy(),
                        })?;
                    }
                    return Err(Error::YAJLishParseError {
                        error: format!("Invalid JSON due to the following error: {}", error),
                    });
                }
                if let Err(error) = parser.finish_parse() {
                    // never seems to reach parse complete on stream data
                    if !error
                        .to_string()
                        .contains("Did not reach a ParseComplete status")
                    {
                        log::debug!("Parse error, whilst in cleaning up parsing");
                        //stop_sender.send(()).context(ChannelStopSendSnafu {})?;
                        if let Some(final_output_path) = final_output_path {
                            remove_dir_all(final_output_path).context(FlattererRemoveDirSnafu {
                                filename: final_output_path.to_string_lossy(),
                            })?;
                        }
                        return Err(Error::YAJLishParseError {
                            error: format!("Invalid JSON due to the following error: {}", error),
                        });
                    }
                }
            }

            top_level_type = handler.top_level_type.clone();

            if !handler.error.is_empty() {
                if let Some(final_output_path) = final_output_path {
                    remove_dir_all(final_output_path).context(FlattererRemoveDirSnafu {
                        filename: final_output_path.to_string_lossy(),
                    })?;
                }
                return Err(Error::FlattererProcessError {
                    message: handler.error,
                });
            }
        }

        if top_level_type == "object" && !options.json_stream {
            let item = Item {
                json: std::str::from_utf8(&outer)
                    .expect("utf8 should be checked by yajl")
                    .to_string(),
                path: vec![],
            };
            item_sender.send(item).context(ChannelItemSnafu {})?;
        }
    };
    Ok(())
}

#[cfg(not(target_family = "wasm"))]
pub fn flatten_simple<R: Read>(
    mut input: BufReader<R>,
    output: String,
    options: Options,
) -> Result<()> {
    let output_path = PathBuf::from(output);

    let mut flat_files = FlatFiles::new(
        output_path.clone().to_string_lossy().to_string(),
        options.clone(),
    )?;

    let mut outer: Vec<u8> = vec![];
    let top_level_type;

    if options.ndjson {
        for line in input.lines() {
            let value = serde_json::from_str(&line.context(FlattererIoSnafu)?).context(SerdeReadSnafu)?;
            flat_files.process_value(value, vec![]);
            flat_files.create_rows()?;
        };
        flat_files.write_files()?;
        return Ok(());
    } else {
        let mut handler = yajlparser::ParseJson::new(
            &mut outer,
            None,
            Some(flat_files),
            options.json_stream,
            500 * 1024 * 1024,
        );

        {
            let mut parser = Parser::new(&mut handler);

            if let Err(error) = parser.parse(&mut input) {
                log::debug!("Parse error, whilst in main parsing");
                remove_dir_all(&output_path).context(FlattererRemoveDirSnafu {
                    filename: output_path.to_string_lossy(),
                })?;
                return Err(Error::YAJLishParseError {
                    error: format!("Invalid JSON due to the following error: {}", error),
                });
            }

            if let Err(error) = parser.finish_parse() {
                // never seems to reach parse complete on stream data
                if !error
                    .to_string()
                    .contains("Did not reach a ParseComplete status")
                {
                    log::debug!("Parse error, whilst in cleaning up parsing");
                    //stop_sender.send(()).context(ChannelStopSendSnafu {})?;
                    remove_dir_all(&output_path).context(FlattererRemoveDirSnafu {
                        filename: output_path.to_string_lossy(),
                    })?;
                    return Err(Error::YAJLishParseError {
                        error: format!("Invalid JSON due to the following error: {}", error),
                    });
                }
            }
        }

        top_level_type = handler.top_level_type.clone();
        flat_files = handler.flatfiles.expect("we added it earlier");

        if !handler.error.is_empty() {
            remove_dir_all(&output_path).context(FlattererRemoveDirSnafu {
                filename: output_path.to_string_lossy(),
            })?;
            return Err(Error::FlattererProcessError {
                message: handler.error,
            });
        }
        if handler.count == 0 {
            return Err(Error::FlattererProcessError {
                message: "The JSON provided as input is not an array of objects".to_string(),
            });
        }
        flat_files.log_info(&format!("Finished processing {} value(s)", handler.count));
    }

    if top_level_type == "object" && !options.json_stream {
        let json_str = std::str::from_utf8(&outer)
            .expect("utf8 should be checked by yajl")
            .to_string();

        let serde_value: Value = serde_json::from_str(&json_str).context(SerdeReadSnafu {})?;

        flat_files.process_value(serde_value, vec![]);
        flat_files.create_rows()?;
    }

    flat_files.write_files()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use logtest::Logger;
    use std::fs::{read_to_string, remove_file};
    use tempfile::TempDir;

    fn test_output(file: &str, output: Vec<&str>, options: Value) {
        let tmp_dir = TempDir::new().unwrap();
        let mut name = file.split('/').last().unwrap().to_string();
        let output_dir = tmp_dir.path().join("output");
        let output_path = output_dir.to_string_lossy().into_owned();

        let mut flatten_options = Options::builder().build();
        flatten_options.force = true;

        flatten_options.csv = true;
        if name != "illegal.json" {
            flatten_options.postgres_connection = "postgres://test:test@localhost/test".into();
            flatten_options.sqlite = true;
            if name != "mixed_case_same.json" {
                flatten_options.parquet = true;
            }
        }
        flatten_options.drop = true;
        flatten_options.sql_scripts = true;

        if let Some(no_link) = options["no_link"].as_bool() {
            flatten_options.no_link = no_link;
            name.push_str("-nolink")
        }

        if let Some(inline) = options["inline"].as_bool() {
            flatten_options.inline_one_to_one = inline;
            name.push_str("-inline")
        }

        if let Some(ndjson) = options["ndjson"].as_bool() {
            flatten_options.ndjson = ndjson;
            name.push_str("-ndjson")
        }

        if let Some(pushdown) = options["pushdown"].as_array() {
            let mut values = vec![];
            for item in pushdown {
                if let Some(value) = item.as_str() {
                    values.push(value.to_owned())
                }
            }
            flatten_options.pushdown = values;
            name.push_str("-pushdown")
        }

        if let Some(threads) = options["threads"].as_i64() {
            flatten_options.threads = usize::try_from(threads).unwrap();
            name.push_str("-threads")
        }

        if let Some(xlsx) = options["xlsx"].as_bool() {
            flatten_options.xlsx = xlsx;
        }

        if let Some(id_prefix) = options["id_prefix"].as_str() {
            flatten_options.id_prefix = id_prefix.into();
            name.push_str("-id_prefix")
        }

        if let Some(table_prefix) = options["table_prefix"].as_str() {
            flatten_options.table_prefix = table_prefix.into();
            name.push_str("-table_prefix")
        }

        if let Some(tables_csv) = options["tables_csv"].as_str() {
            flatten_options.tables_csv = tables_csv.into();

            let tables_only = options["tables_only"].as_bool().unwrap();
            flatten_options.only_tables = tables_only;
            name.push_str("-tablescsv");
            if tables_only {
                name.push_str("-tablesonly")
            }
        }

        if let Some(fields_csv) = options["fields_csv"].as_str() {
            flatten_options.fields_csv = fields_csv.into();

            let fields_only = options["fields_only"].as_bool().unwrap();
            flatten_options.only_fields = fields_only;
            name.push_str("-fieldscsv");
            if fields_only {
                name.push_str("-fieldsonly")
            }
        }

        if let Some(low_disk) = options["low_disk"].as_bool() {
            flatten_options.low_disk = low_disk;
            name.push_str("-low_disk")
        }

        if let Some(path_values) = options["path"].as_array() {
            let path = path_values
                .iter()
                .map(|item| item.as_str().unwrap().to_string())
                .collect();

            name.push_str("-withpath");
            flatten_options.path = path;
        }

        if let Some(json_path) = options["json_path"].as_str() {
            flatten_options.json_path_selector = json_path.into();
            name.push_str("-json_path-");
            name.push_str(json_path)
        }

        flatten_options.json_stream = !file.ends_with(".json") && !file.ends_with(".json.gz");

        flatten_options.postgres_schema = name.clone();

        flatten_options.stats = true;

        // let result = flatten(
        //     BufReader::new(File::open(file).unwrap()),
        //     output_path.clone(),
        //     flatten_options,
        // );

        let mut inputs: Vec<String> = vec![file.into()];

        if let Some(extra_file) = options["extra_file"].as_str() {
            name.push_str("-extrafile");
            inputs.push(extra_file.into());
        }

        let result = flatten_all(
            inputs,
            output_path.clone(),
            flatten_options,
        );

        if let Err(error) = result {
            if let Some(error_text) = options["error_text"].as_str() {
                assert!(error.to_string().contains(error_text), "error was {}", error.to_string())
            } else {
                panic!(
                    "Error raised and there is no error_text to match it. Error was \n{}",
                    error
                );
            }
            assert!(!output_dir.exists());
            return;
        }
        let mut output_tmp_dir = output_dir;
        output_tmp_dir.push("tmp");
        assert!(!output_tmp_dir.exists());

        let mut test_files = vec!["datapackage.json", "fields.csv", "tables.csv"];

        test_files.extend(output);

        for test_file in test_files {
            let new_name = format!("{}-{}", name, test_file);
            if test_file.ends_with(".json") {
                let value: Value = serde_json::from_reader(
                    File::open(format!("{}/{}", output_path.clone(), test_file)).expect(&format!("{test_file} should exist")),
                )
                .unwrap();
                insta::assert_yaml_snapshot!(new_name, &value, {
                    ".resources[].schema.fields[].stats.deciles" => 0,
                    ".resources[].schema.fields[].stats.centiles" => 0,
                });
            } else {
                let file_as_string =
                    read_to_string(format!("{}/{}", output_path.clone(), test_file)).unwrap();

                let output: Vec<_> = file_as_string.lines().collect();
                insta::assert_yaml_snapshot!(new_name, output);
            }
        }
    }

    #[test]
    fn full_test_simple() {
        for extention in ["json", "jl"] {
            test_output(
                &format!("fixtures/basic.{}", extention),
                vec!["csv/main.csv", "csv/platforms.csv", "csv/developer.csv"],
                json!({}),
            )
        }
    }

    #[test]
    fn no_link() {
        for extention in ["json", "jl"] {
            test_output(
                &format!("fixtures/basic.{}", extention),
                vec!["csv/main.csv", "csv/platforms.csv", "csv/developer.csv"],
                json!({"no_link": true, "pushdown": ["id"]}),
            )
        }
    }

    #[test]
    fn full_test_inline() {
        for extention in ["json", "jl"] {
            test_output(
                &format!("fixtures/basic.{}", extention),
                vec!["csv/main.csv", "csv/platforms.csv"],
                json!({"inline": true}),
            );
        }
    }

    #[test]
    fn full_test_in_object() {
        test_output(
            "fixtures/basic_in_object.json",
            vec![
                "csv/main.csv",
                "csv/games.csv",
                "csv/games_platforms.csv",
                "csv/games_developer.csv",
            ],
            json!({}),
        )
    }

    #[test]
    fn full_test_in_object_with_prefix() {
        test_output(
            "fixtures/basic_in_object.json",
            vec![
                "csv/prefix_main.csv",
                "csv/prefix_games.csv",
                "csv/prefix_games_platforms.csv",
                "csv/prefix_games_developer.csv",
            ],
            json!({"table_prefix": "prefix_"}),
        )
    }

    #[test]
    fn full_test_in_object_with_extra() {
        test_output(
            "fixtures/basic_in_object_with_extra.json",
            vec![
                "csv/main.csv",
                "csv/games.csv",
                "csv/games_platforms.csv",
                "csv/games_developer.csv",
            ],
            json!({}),
        )
    }

    #[test]
    fn full_test_in_object_with_multiple() {
        test_output(
            "fixtures/basic_with_multiple.json",
            vec![
                "csv/main.csv",
                "csv/games.csv",
                "csv/games_platforms.csv",
                "csv/moreGames.csv",
                "csv/moreGames_platforms.csv",
            ],
            json!({}),
        )
    }

    #[test]
    fn full_test_mixed_case() {
        test_output(
            "fixtures/mixed_case_same.json",
            vec!["postgresql/postgresql_schema.sql"],
            json!({}),
        )
    }

    #[test]
    fn full_test_select() {
        test_output(
            "fixtures/basic_with_multiple.json",
            vec!["csv/main.csv", "csv/platforms.csv"],
            json!({"path": ["moreGames"]}),
        )
    }

    #[test]
    fn test_id_prefix() {
        test_output(
            &format!("fixtures/basic.{}", "json"),
            vec!["csv/main.csv", "csv/platforms.csv", "csv/developer.csv"],
            json!({"id_prefix": "prefix_"}),
        );
    }

    #[test]
    fn test_tables_csv() {
        for tables_only in [true, false] {
            test_output(
                "fixtures/basic.json",
                vec!["csv/Devs.csv", "csv/Games.csv"],
                json!({"tables_csv": "fixtures/reorder_remove_tables.csv", "tables_only": tables_only}),
            );
        }
    }

    #[test]
    fn test_fields_csv() {
        for fields_only in [true, false] {
            test_output(
                "fixtures/basic.json",
                vec!["csv/main.csv", "csv/developer.csv", "csv/platforms.csv"],
                json!({"fields_csv": "fixtures/fields.csv", "fields_only": fields_only}),
            );
        }
    }

    #[test]
    fn test_invalid() {
        for extention in ["json", "jl"] {
            test_output(
                &format!("fixtures/invalid.{}", extention),
                vec![],
                json!({"error_text": "Invalid JSON due to the following error:"}),
            );
        }
    }

    #[test]
    fn test_array_element() {
        for extention in ["json", "jl"] {
            test_output(
                &format!("fixtures/invalid_array_element.{}", extention),
                vec![],
                json!({"error_text": "Invalid JSON due to the following error:"}),
            );
        }
    }

    #[test]
    fn test_invalid_json_lines() {
        test_output(
            "fixtures/invalid_json_lines.jl",
            vec![],
            json!({"error_text": "Invalid JSON due to the following error:"}),
        );
    }

    #[test]
    fn test_bad_utf8() {
        for extention in ["json", "jl"] {
            test_output(
                &format!("fixtures/bad_utf8.{}", extention),
                vec![],
                json!({"error_text": "Invalid JSON due to the following error:"}),
            );
        }
    }

    #[test]
    fn main_array_literal() {
        test_output(
            "fixtures/main_array_literal.json",
            vec![],
            json!({"error_text": "The JSON provided as input is not an array of objects"}),
        );
        test_output(
            "fixtures/main_array_literal.jl",
            vec![],
            json!({"error_text": "Value at array position 0 is not an object: value is `\"1\"`"}),
        );
    }

    #[test]
    fn main_array_empty() {
        test_output(
            "fixtures/array_empty.json",
            vec![],
            json!({"error_text": "The JSON provided as input is not an array of objects"}),
        );
    }

    #[test]
    fn array_literal() {
        test_output(
            "fixtures/array_literal.json",
            vec![],
            json!({"error_text": "The JSON provided as input is not an array of objects"}),
        );
    }

    #[test]
    fn array_mixed() {
        test_output(
            "fixtures/array_mixed.json",
            vec![],
            json!({"error_text": "Value at array position 1 is not an object: value is `1`"}),
        );
    }

    #[test]
    fn array_object() {
        test_output(
            "fixtures/array_object.json",
            vec![],
            json!({"error_text": "The JSON provided as input is not an array of objects"}),
        );
    }

    #[test]
    fn invalid_xlsx_char() {
        test_output(
            "fixtures/illegal.json",
            vec![],
            json!({"invalid_xlsx_char": true, "xlsx": true}),
        );
    }

    #[test]
    fn test_empty_array() {
        test_output("fixtures/testempty.json", vec![], json!({}));
    }

    #[test]
    fn test_array_str_after_stream() {
        test_output("fixtures/array_str_after_stream.json", vec![], json!({}));
    }

    #[test]
    fn test_pushdown() {
        test_output(
            "fixtures/pushdown.json",
            vec![
                "csv/main.csv",
                "csv/sublist1.csv",
                "csv/sublist1_sublist2.csv",
            ],
            json!({"pushdown": ["a", "b", "c", "d"]}),
        )
    }

    #[test]
    fn test_ndjson_missing_line() {
        test_output(
            "fixtures/missing_line.ndjson",
            vec![],
            json!({"ndjson": true}),
        )
    }

    #[test]
    fn test_multi_file() {
        test_output(
            "fixtures/basic.json",
            vec![],
            json!({"extra_file": "fixtures/basic.json"}),
        )
    }

    #[test]
    fn test_low_disk() {
        test_output(
            "fixtures/basic.json",
            vec![],
            json!({"low_disk": true}),
        )
    }

    #[test]
    fn test_json_path() {
        test_output(
            "fixtures/basic.json",
            vec![],
            json!({"json_path": "$[?(@.title == 'B Game')]"}),
        )
    }

    #[test]
    fn test_json_path_condition() {
        test_output(
            "fixtures/basic_for_query.json",
            vec![],
            json!({"json_path": "$[?(@.rating.code == 'A' && @.rating.name == 'Adult')]"}),
        )
    }

    #[test]
    fn test_http_input() {
        test_output(
            "https://gist.githubusercontent.com/kindly/820c6b5fd49374bfaff5f961d16766ae/raw/031f5e7d33e84f385df447a9991308ad679539ba/basic_http.json",
            vec![],
            json!({}),
        )
    }

    #[test]
    fn test_gz_input() {
        test_output(
            "fixtures/basic.json.gz",
            vec![],
            json!({}),
        )
    }

    #[test]
    fn test_s3_input() {
        if std::env::var("AWS_DEFAULT_REGION").is_ok() {
            test_output(
                "s3://flatterer-test/data/basic.json",
                vec![],
                json!({}),
            );

            test_output(
                "s3://flatterer-test/data/basic.json.gz",
                vec![],
                json!({}),
            )
        }
    }

    #[test]
    fn test_s3() {
        if std::env::var("AWS_DEFAULT_REGION").is_ok() {
            let options = Options::builder()
            .parquet(true)
            .json_stream(true)
            .build();

            flatten_all(
                vec!["fixtures/daily_16.json".into()], // reader
                "s3://flatterer-test/6".into(),                       // output directory
                options,
            )
            .unwrap();

            let options = Options::builder()
            .json_stream(true)
            .build();

            flatten_all(
                vec!["fixtures/daily_16.json".into()], // reader
                "s3://flatterer-test/6".into(),                       // output directory
                options,
            )
            .unwrap();
        }
    }

    #[test]
    fn test_download_upload_s3() {
        if std::env::var("AWS_DEFAULT_REGION").is_ok() {
            let options = Options::builder()
            .parquet(true)
            .ndjson(true)
            .force(true)
            .build();

            flatten_all(
                //vec!["statements.2023-03-08T11:08:56Z.jsonl.gz".into()], // reader
                //vec!["statsmall.jsonl".into()], // reader
                //vec!["statements.2023-03-08T11:08:56Z.jsonl.gz".into()], // reader
                //vec!["https://oo-register-production.s3-eu-west-1.amazonaws.com/public/exports/statements.2023-03-08T11:08:56Z.jsonl.gz".into()], // reader
                vec!["https://flatterer-test.s3.eu-west-2.amazonaws.com/data/daily_16.json.gz".into()], // reader
                "s3://flatterer-test/1".into(),                       // output directory
                options,
            )
            .unwrap();
        }
    }



    #[test]
    fn check_nesting() {
        let myjson = json!({
            "a": "a",
            "c": ["a", "b", "c"],
            "d": {"da": "da", "db": "2005-01-01"},
            "e": [{"ea": 1, "eb": "eb2"},
                  {"ea": 2, "eb": "eb2"}],
        });

        let tmp_dir = TempDir::new().unwrap();

        let mut flat_files = FlatFiles::new_with_defaults(
            tmp_dir.path().join("output").to_string_lossy().into_owned(),
        )
        .unwrap();

        flat_files.process_value(myjson.clone(), vec![]);

        insta::assert_yaml_snapshot!(&flat_files.table_rows);

        flat_files.create_rows().unwrap();

        assert_eq!(
            json!({"e": [],"main": []}),
            serde_json::to_value(&flat_files.table_rows).unwrap()
        );

        insta::assert_yaml_snapshot!(&flat_files.table_metadata,
                                     {".*.output_path" => "[path]"});

        flat_files.process_value(myjson, vec![]);
        flat_files.create_rows().unwrap();

        assert_eq!(
            json!({"e": [],"main": []}),
            serde_json::to_value(&flat_files.table_rows).unwrap()
        );

        insta::assert_yaml_snapshot!(&flat_files.table_metadata,
                                     {".*.output_path" => "[path]"});
    }

    #[test]
    fn test_inline_o2o_when_o2o() {
        let json1 = json!({
            "id": "1",
            "e": [{"ea": 1, "eb": "eb2"}]
        });
        let json2 = json!({
            "id": "2",
            "e": [{"ea": 2, "eb": "eb2"}],
        });

        let tmp_dir = TempDir::new().unwrap();

        let mut flat_files = FlatFiles::new_with_defaults(
            tmp_dir.path().join("output").to_string_lossy().into_owned(),
        )
        .unwrap();

        flat_files.options.inline_one_to_one = true;

        flat_files.process_value(json1, vec![]);
        flat_files.process_value(json2, vec![]);

        flat_files.create_rows().unwrap();
        flat_files.mark_ignore();

        insta::assert_yaml_snapshot!(&flat_files.table_metadata,
                                     {".*.output_path" => "[path]"});
    }

    #[test]
    fn test_inline_o2o_when_o2m() {
        let json1 = json!({
            "id": "1",
            "e": [{"ea": 1, "eb": "eb2"}]
        });
        let json2 = json!({
            "id": "2",
            "e": [{"ea": 2, "eb": "eb2"}, {"ea": 3, "eb": "eb3"}],
        });

        let tmp_dir = TempDir::new().unwrap();
        let mut flat_files = FlatFiles::new_with_defaults(
            tmp_dir.path().join("output").to_string_lossy().into_owned(),
        )
        .unwrap();

        flat_files.options.inline_one_to_one = true;

        flat_files.process_value(json1, vec![]);
        flat_files.process_value(json2, vec![]);

        flat_files.create_rows().unwrap();
        flat_files.mark_ignore();

        insta::assert_yaml_snapshot!(&flat_files.table_metadata,
                                     {".*.output_path" => "[path]"});
    }

    #[test]
    fn test_xlxs_title() {
        let cases =
            vec![
            ("a title",
             "a title"),
            ("this title is too long so it needs to be truncated",
             "this title is too long so it ne"),
            ("this_title_is_too_long_so_it_needs_to_be_truncated",
             "t_t_i_t_l_s_i_n_t_b_truncated"),
            ("this_title_is_too_long_so_it_needs_to_be_truuuuuuuuuuncated",
             "truuuuuuuuuuncated"), // can only take last part
            ("this_title_is_too_long_so_it_needs_to_be_truuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuncated",
             "truuuuuuuuuuuuuuuuuuuuuuuuuuuuu"), // can only take last part truncated
            ("contracts_implementation_transactions_payer",
             "contrac_impleme_transac_payer"),
            ("contracts_implementation_transactions_payee",
             "contrac_impleme_transac_payee"),
            ("grants_recipientOrganization_location",
             "grants_recipientO_location"),
            ("releases_tender_items_attributes",
             "releas_tender_items_attributes")
        ];

        for case in cases {
            let truncated_title = truncate_xlsx_title(case.0.to_string(), "_");
            assert_eq!(truncated_title, case.1);
        }
    }

    #[test]
    fn test_large_cell() {
        let options = Options::builder().force(true).xlsx(true).build();

        let tmp_dir = TempDir::new().unwrap();

        let mut logger = Logger::start();

        flatten(
            Box::new(BufReader::new(File::open("fixtures/large_cell.json").unwrap())), // reader
            tmp_dir.path().to_string_lossy().into(),                         // output directory
            options,
        )
        .unwrap();

        let mut all_logs = vec![];
        while let Some(log) = logger.pop() {
            all_logs.push(log.args().to_string())
        }
        assert!(all_logs.contains(&"WARNING: Cell larger than 32767 chararcters which is too large for XLSX format. The cell will be truncated, so some data will be missing.".to_string()));

        let options = Options::builder().force(true).xlsx(true).threads(2).build();

        let tmp_dir = TempDir::new().unwrap();

        flatten(
            Box::new(BufReader::new(File::open("fixtures/large_cell.json").unwrap())), // reader
            tmp_dir.path().to_string_lossy().into(),                         // output directory
            options,
        )
        .unwrap();

        let mut all_logs = vec![];
        while let Some(log) = logger.pop() {
            all_logs.push(log.args().to_string())
        }
        assert!(all_logs.contains(&"WARNING: Cell larger than 32767 chararcters which is too large for XLSX format. The cell will be truncated, so some data will be missing.".to_string()))
    }

    #[test]
    fn test_multi() {
        let options = Options::builder()
            .json_stream(true)
            .parquet(true)
            .sqlite(true)
            .xlsx(true)
            .postgres_connection("postgres://test:test@localhost/test".into())
            .drop(true)
            .threads(0)
            .force(true)
            .build();

        let tmp_dir = TempDir::new().unwrap();

        flatten(
            Box::new(BufReader::new(File::open("fixtures/daily_16.json").unwrap())), // reader
            tmp_dir.path().to_string_lossy().into(),                       // output directory
            options,
        )
        .unwrap();

        let value: Value =
            serde_json::from_reader(File::open(tmp_dir.path().join("datapackage.json")).unwrap())
                .unwrap();

        insta::assert_yaml_snapshot!(&value);

        assert_eq!(
            BufReader::new(File::open(tmp_dir.path().join("csv/main.csv")).unwrap())
                .lines()
                .count(),
            5000
        );
        assert!(PathBuf::from(tmp_dir.path().join("parquet/main.parquet")).exists());
        assert!(PathBuf::from(tmp_dir.path().join("sqlite.db")).exists());
        assert!(PathBuf::from(tmp_dir.path().join("output.xlsx")).exists());
    }

    #[test]
    fn test_multi_speed() {
        let options = Options::builder().ndjson(true).threads(0).force(true).build();

        let tmp_dir = TempDir::new().unwrap();

        flatten(
            Box::new(BufReader::new(File::open("fixtures/daily_16.json").unwrap())), // reader
            tmp_dir.path().to_string_lossy().into(),                       // output directory
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_simple_speed() {
        let options = Options::builder().json_stream(true).force(true).build();

        let tmp_dir = TempDir::new().unwrap();

        flatten_simple(
            BufReader::new(File::open("fixtures/daily_16.json").unwrap()), // reader
            tmp_dir.path().to_string_lossy().into(),                       // output directory
            options,
        )
        .unwrap();
    }


    #[test]
    fn test_evolve() {
        remove_file("/tmp/evolve.sqlite").ok();

        let options = Options::builder()
            .sqlite(true)
            .postgres_connection("postgres://test:test@localhost/test".into())
            .sqlite_path("/tmp/evolve.sqlite".into())
            .postgres_schema("evolve_basic".into())
            .drop(true)
            .evolve(true)
            .force(true)
            .build();

        let tmp_dir = TempDir::new().unwrap();

        flatten(
            Box::new(BufReader::new(File::open("fixtures/basic.json").unwrap())),
            tmp_dir.path().to_string_lossy().into(),
            options,
        )
        .unwrap();

        let options = Options::builder()
            .sqlite(true)
            .postgres_connection("postgres://test:test@localhost/test".into())
            .sqlite_path("/tmp/evolve.sqlite".into())
            .evolve(true)
            .postgres_schema("evolve_basic".into())
            .force(true)
            .build();

        flatten(
            Box::new(BufReader::new(File::open("fixtures/basic_evolve.json").unwrap())),
            tmp_dir.path().to_string_lossy().into(),
            options,
        )
        .unwrap();
    }

    fn test_output_memory(file: &str, output: Vec<&str>, mut options: Options) {
        let name = file.split('/').last().unwrap().to_string();

        options.memory = true;
        // options.xlsx = true;
        options.csv = true;

        options.json_stream = true;

        let file_string = std::fs::read_to_string(file).unwrap();

        let result = flatten_to_memory(BufReader::new(file_string.as_bytes()), options).unwrap();

        for file in output {
            let mut lines = vec![];

            let reader = flate2::read::GzDecoder::new(result.csv_memory_gz[file].as_slice());

            let reader = ReaderBuilder::new().has_headers(false).from_reader(reader);

            for line in reader.into_records() {
                let string_record = line.unwrap();
                let line_vec: Vec<String> =
                    string_record.into_iter().map(|a| a.to_owned()).collect();
                lines.push(line_vec)
            }

            insta::assert_yaml_snapshot!(format!("{file}-{name}"), lines);
        }
    }

    #[test]
    fn full_test_in_object_memory() {
        test_output_memory(
            "fixtures/basic_in_object.json",
            vec![
                "main.csv",
                "games.csv",
                "games_platforms.csv",
                "games_developer.csv",
            ],
            Options::builder().build(),
        )
    }

}


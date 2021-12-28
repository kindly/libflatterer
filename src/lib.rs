//! libflatterer - Lib to make JSON flatterer.
//!
//! Currently for use only for [flatterer](https://flatterer.opendata.coop/).
//!
//! This is all an internal API exposed to the above tool, so fairly unstable for the time being
//! and extra argument could be added with minor version bump.
//!
//! ```
//! use tempfile::TempDir;
//! use std::fs::File;
//! use libflatterer::{FlatFiles, flatten};
//! use std::io::BufReader;
//!
//! let tmp_dir = TempDir::new().unwrap();
//! let output_dir = tmp_dir.path().join("output");
//! let output_path = output_dir.to_string_lossy().into_owned();
//! let flat_files = FlatFiles::new(
//!     output_path, // output directory
//!     true, // output directory
//!     true, // make csv
//!     true, // make xlsx
//!     "main".to_string(), // main table name
//!     vec![], // list of json paths to omit object as if it was array
//!     false, // inline one to one tables if possible
//!     "".to_string(), // path or uri to JSONSchema
//!     "prefix_".to_string(), // table prefix
//!     "_".to_string(), // path seperator
//!     "".to_string(), // schema titles
//! ).unwrap();
//!
//! flatten(
//!    BufReader::new(File::open("fixtures/basic.json").unwrap()), // reader
//!    flat_files, // FlatFile instance.
//!    vec![]); // Path to array.
//!```
mod postgresql;
mod schema_analysis;

use indexmap::IndexMap as HashMap;
use std::convert::TryInto;
use std::fmt;
use std::fs::{create_dir_all, remove_dir_all, File};
use std::io::{self, BufReader, Read, Write};
use std::path::PathBuf;
use std::{panic, thread};
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::{bounded, Receiver, SendError, Sender};
use csv::{ByteRecord, Reader, ReaderBuilder, Writer, WriterBuilder};
use itertools::Itertools;
use regex::Regex;
use log::{warn, info};
use serde::{Deserialize, Serialize};
use serde_json::{json, Deserializer, Map, Value};
use smallvec::{smallvec, SmallVec};
use smartstring::alias::String as SmartString;
use snafu::{Backtrace, ResultExt, Snafu};
use xlsxwriter::Workbook;
use yajlish::ndjson_handler::NdJsonHandler;
use yajlish::Parser;

pub use yajlish::ndjson_handler::Selector;

lazy_static::lazy_static! {
    pub static ref TERMINATE: AtomicBool = AtomicBool::new(false);
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
        "Output XLSX will have too may rows {} in sheet `{}`, maximum allowed 1048576",
        rows,
        sheet
    ))]
    XLSXTooManyRows { rows: u32, sheet: String },
    #[snafu(display(
        "Output XLSX will have too may columns {} in sheet `{}`, maximum allowed 65536",
        columns,
        sheet
    ))]
    XLSXTooManyColumns { columns: usize, sheet: String },
    #[snafu(display("Terminated"))]
    Terminated {},
    #[snafu(display("Json Dereferencing Failed"))]
    JSONRefError { source: schema_analysis::Error },
    #[snafu(display("Error writing to CSV file {}", filepath.to_string_lossy()))]
    FlattererCSVWriteError {
        filepath: PathBuf,
        source: csv::Error,
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
    #[snafu(display("Could not write file {}", filename))]
    SerdeWriteError {
        filename: String,
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid JSON due to the following error: {}", source))]
    SerdeReadError { source: serde_json::Error },
    #[snafu(display("{}", message))]
    FlattererProcessError { message: String },
    #[snafu(display("Error with writing XLSX file"))]
    FlattererXLSXError { source: xlsxwriter::XlsxError },
    #[snafu(display("Could not convert usize to int"))]
    FlattererIntError { source: std::num::TryFromIntError },
    #[snafu(display("YAJLish parse error: {}", error))]
    YAJLishParseError { error: String },
    #[snafu(display(""))]
    ChannelSendError { source: SendError<Value> },
    #[snafu(display(""))]
    ChannelBufSendError { source: SendError<(Vec<u8>, bool)> },
    #[snafu(display(""))]
    ChannelStopSendError { source: SendError<()> },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Expreses a JSON path element, either a string  or a number.
#[derive(Hash, Clone, Debug)]
pub enum PathItem {
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

#[derive(Debug)]
pub enum TmpCSVWriter {
    Disk(csv::Writer<File>),
    None(),
}

#[derive(Debug)]
pub struct FlatFiles {
    output_path: PathBuf,
    csv: bool,
    pub xlsx: bool,
    pub main_table_name: String,
    emit_obj: SmallVec<[SmallVec<[String; 5]>; 5]>,
    row_number: u128,
    date_regexp: Regex,
    table_rows: HashMap<String, Vec<Map<String, Value>>>,
    tmp_csvs: HashMap<String, TmpCSVWriter>,
    table_metadata: HashMap<String, TableMetadata>,
    pub only_fields: bool,
    pub inline_one_to_one: bool,
    one_to_many_arrays: SmallVec<[SmallVec<[SmartString; 5]>; 5]>,
    one_to_one_arrays: SmallVec<[SmallVec<[SmartString; 5]>; 5]>,
    pub table_prefix: String,
    pub path_separator: String,
    order_map: HashMap<String, usize>,
    field_titles_map: HashMap<String, String>,
    pub preview: usize,
    only_tables: bool,
    table_order: HashMap<String, String>,
}

#[derive(Serialize, Debug)]
pub struct TableMetadata {
    field_type: Vec<String>,
    fields: Vec<String>,
    field_counts: Vec<u32>,
    rows: u32,
    ignore: bool,
    ignore_fields: Vec<bool>,
    order: Vec<usize>,
    field_titles: Vec<String>,
    table_name_with_separator: String,
    output_path: PathBuf,
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
            field_counts: vec![],
            field_type: vec![],
            rows: 0,
            ignore: false,
            ignore_fields: vec![],
            order: vec![],
            field_titles: vec![],
            table_name_with_separator,
            output_path,
        }
    }
}

#[derive(Debug, Deserialize)]
struct FieldsRecord {
    table_name: String,
    field_name: String,
    field_type: Option<String>,
    field_title: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TablesRecord {
    table_name: String,
    table_title: String,
}

struct JLWriter {
    pub buf: Vec<u8>,
    pub buf_sender: Sender<(Vec<u8>, bool)>,
}

impl Write for JLWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf == [b'\n'] {
            if let Err(_) = self.buf_sender.send((self.buf.clone(), false)) {
                log::error!("Unable to process any data, most likely caused by termination of worker");
                return Err(io::Error::new(io::ErrorKind::Other, "Unable to process any data, most likely caused by termination of worker"));
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
        FlatFiles::new(
            output_dir,
            true,
            false,
            false,
            "main".to_string(),
            vec![],
            false,
            "".to_string(),
            "".to_string(),
            "_".to_string(),
            "".to_string(),
        )
    }

    pub fn new(
        output_dir: String,
        csv: bool,
        xlsx: bool,
        force: bool,
        main_table_name: String,
        emit_obj: Vec<Vec<String>>,
        inline_one_to_one: bool,
        schema: String,
        table_prefix: String,
        path_separator: String,
        schema_titles: String,
    ) -> Result<Self> {
        smartstring::validate();

        let output_path = PathBuf::from(output_dir);
        if output_path.is_dir() {
            if force {
                remove_dir_all(&output_path).context(FlattererRemoveDir {
                    filename: output_path.to_string_lossy(),
                })?;
            } else {
                return Err(Error::FlattererDirExists { dir: output_path });
            }
        }

        let tmp_path = output_path.join("tmp");
        create_dir_all(&tmp_path).context(FlattererCreateDir {
            filename: tmp_path.to_string_lossy(),
        })?;

        let smallvec_emit_obj: SmallVec<[SmallVec<[String; 5]>; 5]> = smallvec![];

        let mut flat_files = FlatFiles {
            output_path,
            csv,
            xlsx,
            main_table_name: [table_prefix.clone(), main_table_name].concat(),
            emit_obj: smallvec_emit_obj,
            row_number: 0,
            date_regexp: Regex::new(r"^([1-3]\d{3})-(\d{2})-(\d{2})([T ](\d{2}):(\d{2}):(\d{2}(?:\.\d*)?)((-(\d{2}):(\d{2})|Z)?))?$").unwrap(),
            table_rows: HashMap::new(),
            tmp_csvs: HashMap::new(),
            table_metadata: HashMap::new(),
            only_fields: false,
            inline_one_to_one,
            one_to_many_arrays: SmallVec::new(),
            one_to_one_arrays: SmallVec::new(),
            table_prefix,
            path_separator,
            order_map: HashMap::new(),
            field_titles_map: HashMap::new(),
            preview: 0,
            only_tables: false,
            table_order: HashMap::new(),
        };

        flat_files.set_csv(csv)?;

        if !schema.is_empty() {
            flat_files.set_schema(schema, schema_titles)?;
        };

        flat_files.set_emit_obj(emit_obj)?;

        Ok(flat_files)
    }

    fn set_csv(&mut self, csv: bool) -> Result<()> {
        self.csv = csv;
        let csv_path = self.output_path.join("csv");
        if csv {
            if !csv_path.is_dir() {
                create_dir_all(&csv_path).context(FlattererCreateDir {
                    filename: csv_path.to_string_lossy(),
                })?;
            }
        } else if csv_path.is_dir() {
            remove_dir_all(&csv_path).context(FlattererRemoveDir {
                filename: csv_path.to_string_lossy(),
            })?;
        }
        Ok(())
    }

    fn set_schema(&mut self, schema: String, schema_titles: String) -> Result<()> {
        let schema_analysis =
            schema_analysis::schema_analysis(&schema, &self.path_separator, schema_titles)
                .context(JSONRefError {})?;
        self.order_map = schema_analysis.field_order_map;
        self.field_titles_map = schema_analysis.field_titles_map;
        Ok(())
    }

    fn set_emit_obj(&mut self, emit_obj: Vec<Vec<String>>) -> Result<()> {
        for emit_vec in emit_obj {
            self.emit_obj.push(SmallVec::from_vec(emit_vec))
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
        one_to_one_key: bool,
    ) -> Option<Map<String, Value>> {
        let mut table_name = String::new();
        if emit {
            table_name = [
                self.table_prefix.clone(),
                no_index_path.join(&self.path_separator),
            ]
            .concat();

            if no_index_path.is_empty() {
                table_name = self.main_table_name.clone();
            }

            if !self.table_rows.contains_key(&table_name) {
                self.table_rows.insert(table_name.clone(), vec![]);
            }
        }

        let mut to_insert: SmallVec<[(String, Value); 30]> = smallvec![];
        let mut to_delete: SmallVec<[String; 30]> = smallvec![];

        let mut one_to_one_array: SmallVec<[(String, Value); 30]> = smallvec![];

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
                    let new_value = json!(keys.join(","));
                    to_insert.push((key.clone(), new_value))
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

                        if self.inline_one_to_one
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
                            if !one_to_one_key {
                                self.handle_obj(
                                    my_obj,
                                    true,
                                    new_full_path,
                                    new_no_index_path,
                                    new_one_to_many_full_paths,
                                    new_one_to_many_no_index_paths,
                                    false,
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
                    );
                    if let Some(mut my_obj) = new_obj {
                        for (new_key, new_value) in my_obj.iter_mut() {
                            let mut object_key = String::with_capacity(100);
                            object_key.push_str(key);
                            object_key.push_str(&self.path_separator);
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

    pub fn process_obj(
        &mut self,
        mut obj: Map<String, Value>,
        table_name: String,
        one_to_many_full_paths: SmallVec<[SmallVec<[PathItem; 10]>; 5]>,
        one_to_many_no_index_paths: SmallVec<[SmallVec<[SmartString; 5]>; 5]>,
    ) {
        let mut path_iter = one_to_many_full_paths
            .iter()
            .zip(one_to_many_no_index_paths)
            .peekable();

        if one_to_many_full_paths.is_empty() {
            obj.insert(
                String::from("_link"),
                Value::String(self.row_number.to_string()),
            );
        }

        while let Some((full, no_index)) = path_iter.next() {
            if path_iter.peek().is_some() {
                obj.insert(
                    [
                        "_link_".to_string(),
                        no_index.iter().join(&self.path_separator),
                    ]
                    .concat(),
                    Value::String(
                        [
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
                            self.row_number.to_string(),
                            ".".to_string(),
                            full.iter().join("."),
                        ]
                        .concat(),
                    ),
                );
            }
        }

        if table_name != self.main_table_name {
            obj.insert(
                ["_link_", &self.main_table_name].concat(),
                Value::String(self.row_number.to_string()),
            );
        };

        let current_list = self.table_rows.get_mut(&table_name).unwrap(); //we added table_row already
        current_list.push(obj);
    }

    pub fn create_rows(&mut self) -> Result<()> {
        for (table, rows) in self.table_rows.iter_mut() {
            if !self.tmp_csvs.contains_key(table) {
                if self.csv || self.xlsx {
                    let output_path = self.output_path.join(format!("tmp/{}.csv", table));
                    self.tmp_csvs.insert(
                        table.clone(),
                        TmpCSVWriter::Disk(
                            WriterBuilder::new()
                                .flexible(true)
                                .from_path(output_path.clone())
                                .context(FlattererCSVWriteError {
                                    filepath: &output_path,
                                })?,
                        ),
                    );
                } else {
                    self.tmp_csvs.insert(table.clone(), TmpCSVWriter::None());
                }
            }

            if !self.table_metadata.contains_key(table) {
                let output_path = self.output_path.join(format!("tmp/{}.csv", table));
                self.table_metadata.insert(
                    table.clone(),
                    TableMetadata::new(
                        table,
                        &self.main_table_name,
                        &self.path_separator,
                        &self.table_prefix,
                        output_path,
                    ),
                );
                if !self.only_tables && !self.table_order.contains_key(table) {
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
                        output_row.push(value_convert(
                            value.take(),
                            &mut table_metadata.field_type,
                            num,
                            &self.date_regexp,
                        ));
                    } else {
                        output_row.push("".to_string());
                    }
                }
                for (key, value) in row {
                    if !table_metadata.fields.contains(key) && !self.only_fields {
                        table_metadata.fields.push(key.clone());
                        table_metadata.field_counts.push(1);
                        table_metadata.field_type.push("".to_string());
                        table_metadata.ignore_fields.push(false);
                        let full_path =
                            format!("{}{}", table_metadata.table_name_with_separator, key);

                        if let Some(title) = self.field_titles_map.get(&full_path) {
                            table_metadata.field_titles.push(title.clone());
                        } else {
                            table_metadata.field_titles.push(key.clone());
                        }

                        output_row.push(value_convert(
                            value.take(),
                            &mut table_metadata.field_type,
                            table_metadata.fields.len() - 1,
                            &self.date_regexp,
                        ));
                    }
                }
                if !output_row.is_empty() {
                    table_metadata.rows += 1;
                    if let TmpCSVWriter::Disk(writer) = writer {
                        writer
                            .write_record(&output_row)
                            .context(FlattererCSVWriteError {
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

    pub fn process_value(&mut self, value: Value) {
        if let Value::Object(obj) = value {
            self.handle_obj(
                obj,
                true,
                smallvec![],
                smallvec![],
                smallvec![],
                smallvec![],
                false,
            );
            self.row_number += 1;
        }
    }

    pub fn use_tables_csv(&mut self, filepath: String, only_tables: bool) -> Result<()> {
        self.only_tables = only_tables;
        let mut tables_reader = Reader::from_path(&filepath).context(FlattererCSVReadError {
            filepath: &filepath,
        })?;
        for row in tables_reader.deserialize() {
            let row: TablesRecord = row.context(FlattererCSVReadError {
                filepath: &filepath,
            })?;
            self.table_order.insert(row.table_name, row.table_title);
        }
        Ok(())
    }

    pub fn use_fields_csv(&mut self, filepath: String, only_fields: bool) -> Result<()> {
        let mut fields_reader = Reader::from_path(&filepath).context(FlattererCSVReadError {
            filepath: &filepath,
        })?;

        self.only_fields = only_fields;

        for row in fields_reader.deserialize() {
            let row: FieldsRecord = row.context(FlattererCSVReadError {
                filepath: &filepath,
            })?;

            if !self.table_metadata.contains_key(&row.table_name) {
                let output_path = self
                    .output_path
                    .join(format!("tmp/{}.csv", &row.table_name));
                self.table_metadata.insert(
                    row.table_name.clone(),
                    TableMetadata::new(
                        &row.table_name,
                        &self.main_table_name,
                        &self.path_separator,
                        &self.table_prefix,
                        output_path,
                    ),
                );
                if !self.only_tables {
                    self.table_order
                        .insert(row.table_name.clone(), row.table_name.clone());
                }
            }
            let table_metadata = self.table_metadata.get_mut(&row.table_name).unwrap(); //key known
            table_metadata.fields.push(row.field_name.clone());
            table_metadata.field_counts.push(0);
            table_metadata
                .field_type
                .push(row.field_type.unwrap_or_else(|| "".to_string()));
            table_metadata.ignore_fields.push(false);
            match row.field_title {
                Some(field_title) => table_metadata.field_titles.push(field_title),
                None => table_metadata.field_titles.push(row.field_name),
            }
        }

        Ok(())
    }

    pub fn mark_ignore(&mut self) {
        let one_to_many_table_names = self
            .one_to_many_arrays
            .iter()
            .map(|item| item.join(&self.path_separator))
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
                self.table_prefix,
                table_path.iter().join(&self.path_separator)
            );
            if let Some(table_metadata) = self.table_metadata.get_mut(&table_name) {
                table_metadata.ignore = true
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

    pub fn write_files(&mut self) -> Result<()> {
        info!("Analyzing input data");
        self.mark_ignore();
        self.determine_order();

        //remove tables that should not be there from table order.
        self.table_order
            .retain(|key, _| self.table_metadata.contains_key(key));

        for (file, tmp_csv) in self.tmp_csvs.iter_mut() {
            if let TmpCSVWriter::Disk(tmp_csv) = tmp_csv {
                tmp_csv.flush().context(FlattererFileWriteError {
                    filename: file.clone(),
                })?;
            }
        }

        if self.csv {
            self.write_csvs()?;
            self.write_postgresql()?;
            self.write_sqlite()?;
        };

        if self.xlsx {
            self.write_xlsx()?;
        };

        let tmp_path = self.output_path.join("tmp");
        remove_dir_all(&tmp_path).context(FlattererRemoveDir {
            filename: tmp_path.to_string_lossy(),
        })?;
        info!("Writing metadata files");

        self.write_data_package()?;
        self.write_fields_csv()?;
        self.write_tables_csv()?;

        Ok(())
    }

    pub fn write_data_package(&mut self) -> Result<()> {
        let metadata_file = File::create(self.output_path.join("data_package.json")).context(
            FlattererFileWriteError {
                filename: "data_package.json",
            },
        )?;

        let mut resources = vec![];

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self.table_metadata.get(table_name).unwrap();
            let mut fields = vec![];
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let table_order = metadata.order.clone();

            for order in table_order {
                if metadata.ignore_fields[order] {
                    continue;
                }
                let field = json!({
                    "name": metadata.field_titles[order],
                    "type": metadata.field_type[order],
                    "count": metadata.field_counts[order],
                });
                fields.push(field);
            }

            let mut resource = json!({
                "profile": "tabular-data-resource",
                "name": table_title,
                "schema": {
                    "fields": fields,
                    "primaryKey": "_link"
                }
            });
            if self.csv {
                resource.as_object_mut().unwrap().insert(
                    "path".to_string(),
                    Value::String(format!("csv/{}.csv", table_title)),
                );
            }

            resources.push(resource)
        }

        let data_package = json!({
            "profile": "tabular-data-package",
            "resources": resources
        });

        serde_json::to_writer_pretty(metadata_file, &data_package).context(SerdeWriteError {
            filename: "data_package.json",
        })?;

        Ok(())
    }

    pub fn write_tables_csv(&mut self) -> Result<()> {
        let filepath = self.output_path.join("tables.csv");
        let mut table_writer = Writer::from_path(&filepath).context(FlattererCSVWriteError {
            filepath: filepath.clone(),
        })?;
        table_writer
            .write_record(["table_name", "table_title"])
            .context(FlattererCSVWriteError {
                filepath: filepath.clone(),
            })?;
        for (table_name, table_title) in self.table_order.iter() {
            table_writer
                .write_record([table_name, table_title])
                .context(FlattererCSVWriteError {
                    filepath: filepath.clone(),
                })?;
        }

        Ok(())
    }

    pub fn write_fields_csv(&mut self) -> Result<()> {
        let filepath = self.output_path.join("fields.csv");
        let mut fields_writer = Writer::from_path(&filepath).context(FlattererCSVWriteError {
            filepath: filepath.clone(),
        })?;

        fields_writer
            .write_record([
                "table_name",
                "field_name",
                "field_type",
                "field_title",
                "count",
            ])
            .context(FlattererCSVWriteError {
                filepath: filepath.clone(),
            })?;
        for table_name in self.table_order.keys() {
            let metadata = self.table_metadata.get(table_name).unwrap();
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let table_order = metadata.order.clone();
            for order in table_order {
                if metadata.ignore_fields[order] {
                    continue;
                }
                fields_writer
                    .write_record([
                        table_name,
                        &metadata.fields[order],
                        &metadata.field_type[order],
                        &metadata.field_titles[order],
                        &metadata.field_counts[order].to_string(),
                    ])
                    .context(FlattererCSVWriteError {
                        filepath: filepath.clone(),
                    })?;
            }
        }

        Ok(())
    }

    pub fn write_csvs(&mut self) -> Result<()> {
        info!("Writing final CSV files");
        let tmp_path = self.output_path.join("tmp");
        let csv_path = self.output_path.join("csv");

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
                .context(FlattererCSVReadError {
                    filepath: reader_filepath.to_string_lossy(),
                })?;

            let filepath = csv_path.join(format!("{}.csv", table_title));
            info!("    Writing {} row(s) to {}.csv", metadata.rows, table_title);
            if TERMINATE.load(Ordering::SeqCst) {
                return Err(Error::Terminated {})
            };
            let mut csv_writer = WriterBuilder::new().from_path(filepath.clone()).context(
                FlattererCSVWriteError {
                    filepath: filepath.clone(),
                },
            )?;

            let mut non_ignored_fields = vec![];

            let table_order = metadata.order.clone();

            for order in table_order {
                if !metadata.ignore_fields[order] {
                    non_ignored_fields.push(metadata.field_titles[order].clone())
                }
            }

            csv_writer
                .write_record(&non_ignored_fields)
                .context(FlattererCSVWriteError {
                    filepath: filepath.clone(),
                })?;

            let mut output_row = ByteRecord::new();

            for (num, row) in csv_reader.into_byte_records().enumerate() {
                if self.preview != 0 && num == self.preview {
                    break;
                }
                let this_row = row.context(FlattererCSVWriteError {
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
                    .context(FlattererCSVWriteError {
                        filepath: &filepath,
                    })?;
                output_row.clear();
            }
        }

        Ok(())
    }

    pub fn write_xlsx(&mut self) -> Result<()> {
        info!("Writing final XLSX file");
        let tmp_path = self.output_path.join("tmp");

        let workbook = Workbook::new_with_options(
            &self.output_path.join("output.xlsx").to_string_lossy(),
            true,
            Some(&tmp_path.to_string_lossy()),
            false,
        );

        for (table_name, metadata) in self.table_metadata.iter() {
            if metadata.rows > 1048575 {
                return Err(Error::XLSXTooManyRows {
                    rows: metadata.rows,
                    sheet: table_name.clone(),
                });
            }
            if metadata.rows > 65536 {
                return Err(Error::XLSXTooManyColumns {
                    columns: metadata.fields.len(),
                    sheet: table_name.clone(),
                });
            }
        }
        #[allow(clippy::invalid_regex)]
        let invalid_regex = regex::RegexBuilder::new(r"[\000-\010]|[\013-\014]|[\016-\037]")
            .octal(true)
            .build()
            .unwrap();
        //let invalid_regex = regex::Regex::new(r"\u0000").unwrap();

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self.table_metadata.get(table_name).unwrap(); //key known
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let mut new_table_title = table_title.clone();

            if table_name != table_title {
                new_table_title.truncate(31);
            } else {
                new_table_title = truncate_xlsx_title(new_table_title, &self.path_separator);
            }
            if TERMINATE.load(Ordering::SeqCst) {
                return Err(Error::Terminated {})
            };
            info!("    Writing {} row(s) to sheet `{}`", metadata.rows, new_table_title);

            let mut worksheet = workbook
                .add_worksheet(Some(&new_table_title))
                .context(FlattererXLSXError {})?;

            let filepath = tmp_path.join(format!("{}.csv", table_name));
            let csv_reader = ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_path(filepath.clone())
                .context(FlattererCSVReadError {
                    filepath: filepath.to_string_lossy(),
                })?;

            let mut col_index = 0;

            let table_order = metadata.order.clone();

            for order in table_order {
                if !metadata.ignore_fields[order] {
                    let mut title = metadata.field_titles[order].clone();
                    if invalid_regex.is_match(&title) {
                        warn!("Characters found in input JSON that are not allowed in XLSX file or could cause issues. Striping these, so output is possible.");
                        title = invalid_regex.replace_all(&title, "").to_string();
                    }

                    worksheet
                        .write_string(0, col_index, &title, None)
                        .context(FlattererXLSXError {})?;
                    col_index += 1;
                }
            }

            for (row_num, row) in csv_reader.into_records().enumerate() {
                col_index = 0;
                let this_row = row.context(FlattererCSVReadError {
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

                    if invalid_regex.is_match(&cell) {
                        warn!("Character found in JSON that is not allowed in XLSX file. Removing these so output is possible");
                        cell = invalid_regex.replace_all(&cell, "").to_string();
                    }

                    if metadata.field_type[order] == "number" {
                        if let Ok(number) = cell.parse::<f64>() {
                            worksheet
                                .write_number(
                                    (row_num + 1).try_into().context(FlattererIntError {})?,
                                    col_index,
                                    number,
                                    None,
                                )
                                .context(FlattererXLSXError {})?;
                        } else {
                            worksheet
                                .write_string(
                                    (row_num + 1).try_into().context(FlattererIntError {})?,
                                    col_index,
                                    &cell,
                                    None,
                                )
                                .context(FlattererXLSXError {})?;
                        };
                    } else {
                        worksheet
                            .write_string(
                                (row_num + 1).try_into().context(FlattererIntError {})?,
                                col_index,
                                &cell,
                                None,
                            )
                            .context(FlattererXLSXError {})?;
                    }
                    col_index += 1
                }
            }
        }
        workbook.close().context(FlattererXLSXError {})?;

        Ok(())
    }

    pub fn write_postgresql(&mut self) -> Result<()> {
        let postgresql_dir_path = self.output_path.join("postgresql");
        create_dir_all(&postgresql_dir_path).context(FlattererCreateDir {
            filename: postgresql_dir_path.to_string_lossy(),
        })?;

        let mut postgresql_schema = File::create(postgresql_dir_path.join("postgresql_schema.sql"))
            .context(FlattererFileWriteError {
                filename: "postgresql_schema.sql",
            })?;
        let mut postgresql_load = File::create(postgresql_dir_path.join("postgresql_load.sql"))
            .context(FlattererFileWriteError {
                filename: "postgresql_load.sql",
            })?;

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self.table_metadata.get(table_name).unwrap();
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let table_order = metadata.order.clone();
            writeln!(
                postgresql_schema,
                "CREATE TABLE \"{ }\"(",
                table_title.to_lowercase()
            )
            .context(FlattererFileWriteError {
                filename: "postgresql_schema.sql",
            })?;

            let mut fields = Vec::new();
            for order in table_order {
                if metadata.ignore_fields[order] {
                    continue;
                }
                fields.push(format!(
                    "    \"{}\" {}",
                    metadata.field_titles[order].to_lowercase(),
                    postgresql::to_postgresql_type(&metadata.field_type[order])
                ));
            }
            write!(postgresql_schema, "{}", fields.join(",\n")).context(
                FlattererFileWriteError {
                    filename: "postgresql_schema.sql",
                },
            )?;
            write!(postgresql_schema, ");\n\n").context(FlattererFileWriteError {
                filename: "postgresql_schema.sql",
            })?;

            writeln!(
                postgresql_load,
                "\\copy \"{}\" from '{}' with CSV HEADER",
                table_title.to_lowercase(),
                format!("csv/{}.csv", table_title),
            )
            .context(FlattererFileWriteError {
                filename: "postgresql_load.sql",
            })?;
        }

        Ok(())
    }

    pub fn write_sqlite(&mut self) -> Result<()> {
        let sqlite_dir_path = self.output_path.join("sqlite");
        create_dir_all(&sqlite_dir_path).context(FlattererCreateDir {
            filename: sqlite_dir_path.to_string_lossy(),
        })?;

        let mut sqlite_schema = File::create(sqlite_dir_path.join("sqlite_schema.sql")).context(
            FlattererFileWriteError {
                filename: "sqlite_schema.sql",
            },
        )?;
        let mut sqlite_load = File::create(sqlite_dir_path.join("sqlite_load.sql")).context(
            FlattererFileWriteError {
                filename: "sqlite_schema.sql",
            },
        )?;

        writeln!(sqlite_load, ".mode csv ").context(FlattererFileWriteError {
            filename: "sqlite_schema.sql",
        })?;

        for (table_name, table_title) in self.table_order.iter() {
            let metadata = self.table_metadata.get(table_name).unwrap();
            if metadata.rows == 0 || metadata.ignore {
                continue;
            }
            let table_order = metadata.order.clone();
            writeln!(
                sqlite_schema,
                "CREATE TABLE \"{ }\"(",
                table_title.to_lowercase()
            )
            .context(FlattererFileWriteError {
                filename: "sqlite_schema.sql",
            })?;

            let mut fields = Vec::new();
            for order in table_order {
                if metadata.ignore_fields[order] {
                    continue;
                }
                fields.push(format!(
                    "    \"{}\" {}",
                    metadata.field_titles[order].to_lowercase(),
                    postgresql::to_postgresql_type(&metadata.field_type[order])
                ));
            }
            write!(sqlite_schema, "{}", fields.join(",\n")).context(FlattererFileWriteError {
                filename: "sqlite_schema.sql",
            })?;
            write!(sqlite_schema, ");\n\n").context(FlattererFileWriteError {
                filename: "sqlite_schema.sql",
            })?;

            writeln!(
                sqlite_load,
                ".import '{}' {} --skip 1 ",
                format!("csv/{}.csv", table_title),
                table_title.to_lowercase()
            )
            .context(FlattererFileWriteError {
                filename: "sqlite_load.sql",
            })?;
        }

        Ok(())
    }
}

fn value_convert(
    value: Value,
    field_type: &mut Vec<String>,
    num: usize,
    date_re: &Regex,
) -> String {
    //let value_type = output_fields.get("type");
    let value_type = &field_type[num];

    match value {
        Value::String(val) => {
            if value_type != "text" {
                if date_re.is_match(&val) {
                    field_type[num] = "date".to_string();
                } else {
                    field_type[num] = "text".to_string();
                }
            }
            val
        }
        Value::Null => {
            if value_type.is_empty() {
                field_type[num] = "null".to_string();
            }
            "".to_string()
        }
        Value::Number(number) => {
            if value_type != "text" {
                field_type[num] = "number".to_string();
            }
            number.to_string()
        }
        Value::Bool(bool) => {
            if value_type != "text" {
                field_type[num] = "boolean".to_string();
            }
            bool.to_string()
        }
        Value::Array(_) => {
            if value_type != "text" {
                field_type[num] = "text".to_string();
            }
            format!("{}", value)
        }
        Value::Object(_) => {
            if value_type != "text" {
                field_type[num] = "text".to_string();
            }
            format!("{}", value)
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
        let new_part = part[..len_of_part].to_string();
        new_parts.push(new_part);
    }
    new_parts.push(last_part);

    new_parts.join(seperator)
}

pub fn flatten_from_jl<R: Read>(input: R, mut flat_files: FlatFiles) -> Result<FlatFiles> {
    let (value_sender, value_receiver) = bounded(1000);
    let (stop_sender, stop_receiver) = bounded(1);
    let output_path = flat_files.output_path.clone();
    info!("Reading JSON input stream and saving output into temporary CSV files");

    let thread = thread::spawn(move || -> Result<FlatFiles> {
        let mut count = 0;
        for value in value_receiver {
            flat_files.process_value(value);
            flat_files.create_rows()?;
            count += 1;
            if count % 500000 == 0 {
                if TERMINATE.load(Ordering::SeqCst) {
                    log::debug!("Terminating..");
                    return Err(Error::Terminated {})
                };
                if stop_receiver.try_recv().is_ok() {
                    return Ok(flat_files) // This is really an error but it should be handled by main thread.
                }
                info!("Processed {} values so far.", count);
            }
        }
        if stop_receiver.try_recv().is_ok() {
            return Ok(flat_files) // This is really an error but it should be handled by main thread.
        }

        info!("Finished processing {} value(s)", count);

        flat_files.write_files()?;
        Ok(flat_files)
    });

    let stream = Deserializer::from_reader(input).into_iter::<Value>();
    for (num, value_result) in stream.enumerate() {
        if value_result.is_err() {
            stop_sender.send(()).context(ChannelStopSendError {})?;
            remove_dir_all(&output_path).context(FlattererRemoveDir {
                filename: output_path.to_string_lossy(),
            })?;
        }
        let value = value_result.context(SerdeReadError {})?;
        if !value.is_object() {
            stop_sender.send(()).context(ChannelStopSendError {})?;
            remove_dir_all(&output_path).context(FlattererRemoveDir {
                filename: output_path.to_string_lossy(),
            })?;
            return Err(Error::FlattererProcessError {message: format!("The JSON provided as input is not an array of objects: Value at array position {} is not an object: value is `{}`", num, value)});
        };
        if value_sender.send(value).is_err() {
            break
        };
    }
    drop(value_sender);

    match thread.join() {
        Ok(result) => {
            if let Err(err) = result {
                remove_dir_all(&output_path).context(FlattererRemoveDir {
                    filename: output_path.to_string_lossy(),
                })?;
                Err(err)
            } else {
                Ok(result.unwrap())
            }
        }
        Err(err) => panic::resume_unwind(err),
    }
}

type VecBool = (Vec<u8>, bool);

pub fn flatten<R: Read>(
    mut input: BufReader<R>,
    mut flat_files: FlatFiles,
    selectors: Vec<Selector>,
) -> Result<FlatFiles> {
    let (buf_sender, buf_receiver): (Sender<VecBool>, Receiver<VecBool>) = bounded(1000);
    let (stop_sender, stop_receiver) = bounded(1);
    info!("Reading JSON input file and saving output into temporary CSV files");

    let output_path = flat_files.output_path.clone();

    let thread = thread::spawn(move || -> Result<FlatFiles> {
        let mut count = 0;
        for (buf, extra) in buf_receiver.iter() {
            if buf.is_empty() {
                return Err(Error::FlattererProcessError {
                    message: "The JSON provided as input contains an empty array".to_string(),
                });
            }
            if extra {
                log::debug!("Extra data found in input, unspecified behaviour in yajlish");
                let mut extra_detail = "";

                if buf[0] == 123 { // 123 is a `{`
                    extra_detail = ": It looks like you have supplied an object, either supply a correct `--path` or try the `--json-lines` option."
                }

                return Err(Error::FlattererProcessError {
                    message: format!(
                        "{}{}",
                        "The JSON provided as input is not an array of objects", extra_detail
                    ),
                });
            }
            let value = serde_json::from_slice::<Value>(&buf).context(SerdeReadError {})?;

            if !value.is_object() {
                return Err(Error::FlattererProcessError {
                    message: format!(
                        "Value at array position {} is not an object: value is `{}`",
                        count, value
                    ),
                });
            }
            flat_files.process_value(value);
            flat_files.create_rows()?;
            count += 1;
            if count % 500000 == 0 {
                info!("Processed {} values so far.", count);
                if TERMINATE.load(Ordering::SeqCst) {
                    log::debug!("Terminating..");
                    return Err(Error::Terminated {})
                };
                if stop_receiver.try_recv().is_ok() {
                    return Ok(flat_files) // This is really an error but it should be handled by main thread.
                }
            }
        }
        if stop_receiver.try_recv().is_ok() {
            return Ok(flat_files) // This is really an error but it should be handled by main thread.
        }
        if TERMINATE.load(Ordering::SeqCst) {
            return Err(Error::Terminated {})
        };
        info!("Finished processing {} value(s)", count);

        flat_files.write_files()?;
        Ok(flat_files)
    });

    let mut jl_writer = JLWriter {
        buf: vec![],
        buf_sender,
    };
    let mut handler = NdJsonHandler::new(&mut jl_writer, selectors);
    let mut parser = Parser::new(&mut handler);

    if let Err(error) = parser.parse(&mut input) {
        log::debug!("Parse error, whilst in main parsing");
        stop_sender.send(()).context(ChannelStopSendError {})?;
        remove_dir_all(&output_path).context(FlattererRemoveDir {
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
            stop_sender.send(()).context(ChannelStopSendError {})?;
            remove_dir_all(&output_path).context(FlattererRemoveDir {
                filename: output_path.to_string_lossy(),
            })?;
            return Err(Error::YAJLishParseError {
                error: format!("Invalid JSON due to the following error: {}", error),
            });
        }
    }

    if !jl_writer.buf.is_empty() {
        jl_writer
            .buf_sender
            .send((jl_writer.buf.clone(), true))
            .context(ChannelBufSendError {})?;
    }

    drop(jl_writer);

    match thread.join() {
        Ok(result) => {
            if let Err(err) = result {
                log::debug!("Error on thread join {}", err);
                remove_dir_all(&output_path).context(FlattererRemoveDir {
                    filename: output_path.to_string_lossy(),
                })?;
                return Err(err);
            }
            Ok(result.unwrap())
        }
        Err(err) => panic::resume_unwind(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::read_to_string;
    use tempfile::TempDir;

    fn test_output(file: &str, output: Vec<&str>, options: Value) {
        let tmp_dir = TempDir::new().unwrap();
        let output_dir = tmp_dir.path().join("output");
        let output_path = output_dir.to_string_lossy().into_owned();
        let mut flat_files = FlatFiles::new_with_defaults(output_path.clone()).unwrap();

        if let Some(inline) = options["inline"].as_bool() {
            flat_files.inline_one_to_one = inline
        }
        if let Some(xlsx) = options["xlsx"].as_bool() {
            flat_files.xlsx = xlsx
        }
        if let Some(tables_csv) = options["tables_csv"].as_str() {
            let tables_only = options["tables_only"].as_bool().unwrap();

            flat_files
                .use_tables_csv(tables_csv.to_string(), tables_only)
                .unwrap();
        }


        let result;

        if file.ends_with(".json") {
            result = flatten(
                BufReader::new(File::open(file).unwrap()),
                flat_files,
                vec![],
            );
        } else {
            result = flatten_from_jl(BufReader::new(File::open(file).unwrap()), flat_files);
        }

        if let Err(error) = result {
            if let Some(error_text) = options["error_text"].as_str() {
                assert!(error.to_string().contains(error_text))
            } else {
                panic!(
                    "Error raised and there is no error_text to match it. Error was \n{}",
                    error
                );
            }
            assert!(!output_dir.exists());
            return;
        }

        let mut test_files = vec!["data_package.json", "fields.csv", "tables.csv"];

        test_files.extend(output);

        for test_file in test_files {
            if test_file.ends_with(".json") {
                let value: Value = serde_json::from_reader(
                    File::open(format!("{}/{}", output_path.clone(), test_file)).unwrap(),
                )
                .unwrap();
                insta::assert_yaml_snapshot!(&value);
            } else {
                let file_as_string =
                    read_to_string(format!("{}/{}", output_path.clone(), test_file)).unwrap();

                let output: Vec<_> = file_as_string.lines().collect();
                insta::assert_yaml_snapshot!(output);
            }
        }
    }

    #[test]
    fn full_test() {
        for extention in ["json", "jl"] {
            test_output(
                &format!("fixtures/basic.{}", extention),
                vec!["csv/main.csv", "csv/platforms.csv"],
                json!({}),
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
        for extention in ["json", "jl"] {
            test_output(
                &format!("fixtures/main_array_literal.{}", extention),
                vec![],
                json!({"error_text": "The JSON provided as input is not an array of objects"}),
            );
        }
    }

    #[test]
    fn main_array_empty() {
        test_output(
            "fixtures/array_empty.json",
            vec![],
            json!({"error_text": "The JSON provided as input contains an empty array"}),
        );
    }

    #[test]
    fn array_literal() {
        test_output(
            "fixtures/array_literal.json",
            vec![],
            json!({"error_text": "Value at array position 0 is not an object: value is `1`"}),
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
            json!({"error_text": "The JSON provided as input is not an array of objects: It looks like you have supplied an object, either supply a correct `--path` or try the `--json-lines` option."}),
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
    fn check_nesting() {
        let myjson = json!({
            "a": "a",
            "c": ["a", "b", "c"],
            "d": {"da": "da", "db": "2005-01-01"},
            "e": [{"ea": 1, "eb": "eb2"},
                  {"ea": 2, "eb": "eb2"}],
        });

        let tmp_dir = TempDir::new().unwrap();

        let mut flat_files = FlatFiles::new(
            tmp_dir.path().join("output").to_string_lossy().into_owned(),
            true,
            true,
            true,
            "main".to_string(),
            vec![],
            false,
            "".to_string(),
            "".to_string(),
            "_".to_string(),
            "".to_string(),
        )
        .unwrap();

        flat_files.process_value(myjson.clone());

        insta::assert_yaml_snapshot!(&flat_files.table_rows);

        flat_files.create_rows().unwrap();

        assert_eq!(
            json!({"e": [],"main": []}),
            serde_json::to_value(&flat_files.table_rows).unwrap()
        );

        insta::assert_yaml_snapshot!(&flat_files.table_metadata,
                                     {".*.output_path" => "[path]"});

        flat_files.process_value(myjson);
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

        let mut flat_files = FlatFiles::new(
            tmp_dir.path().join("output").to_string_lossy().into_owned(),
            true,
            true,
            true,
            "main".to_string(),
            vec![],
            true,
            "".to_string(),
            "".to_string(),
            "_".to_string(),
            "".to_string(),
        )
        .unwrap();

        flat_files.process_value(json1);
        flat_files.process_value(json2);

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
        let mut flat_files = FlatFiles::new(
            tmp_dir.path().join("output").to_string_lossy().into_owned(),
            true,
            true,
            true,
            "main".to_string(),
            vec![],
            true,
            "".to_string(),
            "".to_string(),
            "_".to_string(),
            "".to_string(),
        )
        .unwrap();

        flat_files.process_value(json1);
        flat_files.process_value(json2);

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
        ];

        for case in cases {
            let truncated_title = truncate_xlsx_title(case.0.to_string(), "_");
            assert_eq!(truncated_title, case.1);
        }
    }
}

use std::io::prelude::*;

use crossbeam_channel::Sender;
use smartstring::alias::String as SmartString;
use yajlish::{Context, Enclosing, Handler, Status};

use crate::FlatFiles;

pub struct ParseJson<W: std::io::Write> {
    pub top_level_type: String,
    in_stream: bool,
    stream_start_open_braces: usize,
    stream_start_open_brackets: usize,
    pub current_object: String,
    no_index_path: Vec<SmartString>,
    pub sender: Option<Sender<Item>>,
    pub flatfiles: Option<FlatFiles>,
    top_level_writer: std::io::BufWriter<W>,
    pub error: String,
    limit: usize,
    pub count: usize,
}

#[derive(Debug)]
pub struct Item {
    pub json: String,
    pub path: Vec<SmartString>,
}

impl<W: std::io::Write> ParseJson<W> {
    pub fn new(
        top_level_writer: W,
        sender: Option<Sender<Item>>,
        flatfiles: Option<FlatFiles>,
        stream: bool,
        limit: usize,
    ) -> ParseJson<W> {
        let bufwriter = std::io::BufWriter::new(top_level_writer);
        ParseJson {
            top_level_type: "".to_string(),
            in_stream: stream,
            stream_start_open_braces: 0,
            stream_start_open_brackets: 0,
            current_object: "".to_string(),
            no_index_path: vec![],
            sender,
            flatfiles,
            top_level_writer: bufwriter,
            error: "".to_string(),
            limit,
            count: 0,
        }
    }
    fn push(&mut self, val: &str) {
        if self.in_stream {
            self.current_object.push_str(val);
        } else {
            self.top_level_writer
                .write_all(val.as_bytes())
                .unwrap_or_else(|err| self.error = err.to_string());
        }
    }

    fn over_limit(&mut self) {
        if self.limit != 0 && self.current_object.len() > self.limit {
            self.error = format!("Object too large, string larger than {}", self.limit);
        }
    }

    fn push_array_comma(&mut self, _ctx: &Context) {
        if _ctx.parser_status() == yajlish::ParserStatus::ArrayNeedVal
            && (self.stream_start_open_braces != _ctx.num_open_braces()
                || self.stream_start_open_brackets != _ctx.num_open_brackets()
                || !self.in_stream)
        {
            self.push(",");
        }
    }
    fn send(&mut self, item: Item) -> Status {
        //tracing::trace!("sent {}", self.count);
        self.count += 1;
        if let Some(sender) = self.sender.as_ref() {
            return match sender.send(item) {
                Ok(_) => Status::Continue,
                Err(error) => {
                    self.error = error.to_string();
                    Status::Abort
                }
            };
        }

        if let Some(flat_files) = self.flatfiles.as_mut() {
            let serde_result = serde_json::from_str(&item.json);

            if serde_result.is_err() && item.json.as_bytes().iter().all(u8::is_ascii_whitespace) {
                return Status::Continue;
            }

            if serde_result.is_err() && !flat_files.options.json_stream {
                self.error = "Error parsing JSON, try the --json-stream option.".into();
                return Status::Abort;
            }

            let value: serde_json::Value = match serde_result {
                Ok(value) => value,
                Err(error) => {
                    self.error = error.to_string();
                    return Status::Abort;
                }
            };

            if !value.is_object() {
                self.error = format!(
                    "Value at array position {} is not an object: value is `{}`",
                    self.count - 1,
                    value
                );
                return Status::Abort;
            }

            if !flat_files.options.path.is_empty() && item.path != flat_files.path {
                return Status::Continue;
            }
            let mut initial_path = vec![];
            if flat_files.path.is_empty() {
                initial_path = item.path.clone()
            }

            flat_files.process_value(value, initial_path);
            match flat_files.create_rows() {
                Ok(_) => {}
                Err(error) => {
                    self.error = error.to_string();
                    return Status::Abort;
                }
            };
            return Status::Continue;
        }
        Status::Continue
    }

    fn send_json(&mut self, _ctx: &Context) -> Status {
        if self.in_stream
            && self.stream_start_open_braces == _ctx.num_open_braces()
            && self.stream_start_open_brackets == _ctx.num_open_brackets()
        {
            let json = std::mem::take(&mut self.current_object);
            return self.send(Item {
                json,
                path: self.no_index_path.clone(),
            });
        }
        Status::Continue
    }
}

impl<W: std::io::Write> Handler for ParseJson<W> {
    fn handle_null(&mut self, _ctx: &Context) -> Status {
        self.push_array_comma(_ctx);
        self.push("null");
        self.over_limit();
        if !self.error.is_empty() {
            return Status::Abort;
        }
        self.send_json(_ctx)
    }

    fn handle_double(&mut self, _ctx: &Context, _val: f64) -> Status {
        self.push_array_comma(_ctx);

        self.push(&format!("{}", _val));
        self.over_limit();
        if !self.error.is_empty() {
            return Status::Abort;
        }
        self.send_json(_ctx)
    }

    fn handle_int(&mut self, _ctx: &Context, _val: i64) -> Status {
        self.push_array_comma(_ctx);

        self.push(&format!("{}", _val));
        self.over_limit();
        if !self.error.is_empty() {
            return Status::Abort;
        }
        self.send_json(_ctx)
    }

    fn handle_bool(&mut self, _ctx: &Context, _boolean: bool) -> Status {
        self.push_array_comma(_ctx);
        let bool_str = match _boolean {
            true => "true",
            false => "false",
        };
        self.push(bool_str);
        self.over_limit();
        if !self.error.is_empty() {
            return Status::Abort;
        }
        self.send_json(_ctx)
    }

    fn handle_string(&mut self, _ctx: &Context, _val: &str) -> Status {
        self.push_array_comma(_ctx);
        self.push(_val);
        self.over_limit();
        if !self.error.is_empty() {
            return Status::Abort;
        }
        self.send_json(_ctx)
    }

    fn handle_start_map(&mut self, _ctx: &Context) -> Status {
        if self.top_level_type.is_empty() {
            self.top_level_type = "object".to_string();
        }
        if let Some(enclosing) = _ctx.last_enclosing() {
            if !self.in_stream
                && enclosing == Enclosing::LeftBracket
                && _ctx.parser_status() == yajlish::ParserStatus::ArrayStart
            {
                self.in_stream = true;
                self.stream_start_open_braces = _ctx.num_open_braces();
                self.stream_start_open_brackets = _ctx.num_open_brackets();
            }
        }
        self.push_array_comma(_ctx);

        self.push("{");

        Status::Continue
    }

    fn handle_end_map(&mut self, _ctx: &Context) -> Status {
        self.push("}");
        if self.no_index_path.len() == _ctx.num_open_braces() {
            self.no_index_path.pop();
        }
        if self.in_stream && self.stream_start_open_braces + 1 == _ctx.num_open_braces() {
            let json = std::mem::take(&mut self.current_object);

            return self.send(Item {
                json,
                path: self.no_index_path.clone(),
            });
        }

        Status::Continue
    }

    fn handle_map_key(&mut self, _ctx: &Context, key: &str) -> Status {
        if self.no_index_path.len() == _ctx.num_open_braces() {
            self.no_index_path.pop();
        }
        self.no_index_path
            .push(SmartString::from(&key[1..key.len() - 1]));
        if _ctx.parser_status() == yajlish::ParserStatus::MapNeedKey {
            self.push(",");
        }

        self.push(key);
        self.push(":");
        self.over_limit();
        if !self.error.is_empty() {
            return Status::Abort;
        }

        Status::Continue
    }

    fn handle_start_array(&mut self, _ctx: &Context) -> Status {
        self.push_array_comma(_ctx);
        if self.top_level_type.is_empty() {
            self.top_level_type = "array".to_string();
        }
        self.push("[");
        Status::Continue
    }

    fn handle_end_array(&mut self, _ctx: &Context) -> Status {
        if self.in_stream
            && self.stream_start_open_braces == _ctx.num_open_braces()
            && self.stream_start_open_brackets == _ctx.num_open_brackets()
        {
            self.in_stream = false;
        }
        self.push("]");
        if self.in_stream
            && self.stream_start_open_braces == _ctx.num_open_braces()
            && self.stream_start_open_brackets == _ctx.num_open_brackets() - 1
        {
            let json = std::mem::take(&mut self.current_object);
            return self.send(Item {
                json,
                path: self.no_index_path.clone(),
            });
        }
        if !self.error.is_empty() {
            return Status::Abort;
        }
        Status::Continue
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crossbeam_channel::bounded;
    use serde_json::Value;
    use yajlish::Parser;

    use super::*;

    fn parse(file: &str, stream: bool) -> (Vec<Value>, Vec<Vec<SmartString>>, String) {
        let mut outer: Vec<u8> = vec![];
        let (sender, receiver) = bounded(1000);
        {
            let mut handler = ParseJson::new(&mut outer, Some(sender), None, stream, 0);
            let mut parser = Parser::new(&mut handler);
            let mut reader = std::io::BufReader::new(File::open(file).unwrap());

            parser.parse(&mut reader).unwrap();
        }
        let mut values = vec![];
        let mut paths = vec![];
        for item in receiver {
            values.push(serde_json::from_str(&item.json).unwrap());
            paths.push(item.path)
        }
        println!("here '{}'", std::str::from_utf8(&outer).unwrap());
        if !stream {
            let _: serde_json::Value =
                serde_json::from_str(std::str::from_utf8(&outer).unwrap()).unwrap();
        }
        return (
            values,
            paths,
            std::str::from_utf8(&outer).unwrap().to_string(),
        );
    }

    #[test]
    fn test_yajlparse_basic() {
        insta::assert_yaml_snapshot!(parse("fixtures/basic.json", false));
    }
    #[test]
    fn test_yajlparse_basic_jl() {
        insta::assert_yaml_snapshot!(parse("fixtures/basic.jl", true));
    }
    #[test]
    fn test_yajlparse_basic_array() {
        insta::assert_yaml_snapshot!(parse("fixtures/yajl_array.json", false));
    }

    #[test]
    fn test_yajlparse_not_object() {
        insta::assert_yaml_snapshot!(parse("fixtures/yajl_array_not_obj.json", false));
    }

    #[test]
    fn test_yajlparse_mixed() {
        insta::assert_yaml_snapshot!(parse("fixtures/yajl_array_mixed.json", false));
    }

    #[test]
    fn test_yajlparse_many_lists() {
        insta::assert_yaml_snapshot!(parse("fixtures/yajl_many_lists.json", false));
    }
}

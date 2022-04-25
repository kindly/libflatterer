use snafu::Snafu;
use std::io::BufReader;
use yajlish::{Context, Handler, Parser, Status};

#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error guessing array location: {}", error))]
    YajlishError { error: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct GuessArray {
    guess: String,
    path_guess: String,
    new: bool,
    new_array: bool,
    keys: Vec<Option<String>>,
}

impl GuessArray {
    fn new() -> GuessArray {
        GuessArray {
            guess: "".to_string(),
            path_guess: "".to_string(),
            new: true,
            new_array: false,
            keys: vec![],
        }
    }
}

impl Handler for GuessArray {
    fn handle_map_key(&mut self, _ctx: &Context, key: &str) -> Status {
        self.keys.pop();
        self.keys.push(Some(key[1..key.len() - 1].to_string()));
        Status::Continue
    }

    fn handle_null(&mut self, _ctx: &Context) -> Status {
        if self.new {
            return Status::Abort;
        }
        self.new = false;
        self.new_array = false;
        Status::Continue
    }

    fn handle_bool(&mut self, _ctx: &Context, _boolean: bool) -> Status {
        if self.new {
            return Status::Abort;
        }
        self.new = false;
        self.new_array = false;
        Status::Continue
    }

    fn handle_double(&mut self, _ctx: &Context, _val: f64) -> Status {
        if self.new {
            return Status::Abort;
        }
        self.new = false;
        self.new_array = false;
        Status::Continue
    }

    fn handle_int(&mut self, _ctx: &Context, _val: i64) -> Status {
        if self.new {
            return Status::Abort;
        }
        self.new = false;
        self.new_array = false;
        Status::Continue
    }

    fn handle_string(&mut self, _ctx: &Context, _val: &str) -> Status {
        if self.new {
            return Status::Abort;
        }
        self.new = false;
        self.new_array = false;
        Status::Continue
    }

    fn handle_start_map(&mut self, _ctx: &Context) -> Status {
        if !self.new && self.keys.is_empty() {
            self.guess = "stream".to_string();
            return Status::Abort;
        }
        if self.new_array && self.path_guess.is_empty() {
            let path_guess: Vec<String> = self.keys.iter().filter_map(|i| i.to_owned()).collect();
            self.path_guess = path_guess.join("/");
        }
        self.new = false;
        self.new_array = false;
        self.keys.push(None);
        Status::Continue
    }

    fn handle_end_map(&mut self, _ctx: &Context) -> Status {
        self.keys.pop();
        Status::Continue
    }

    fn handle_start_array(&mut self, _ctx: &Context) -> Status {
        if self.new {
            self.guess = "top_array".to_string();
            return Status::Abort;
        }
        self.new = false;
        self.new_array = true;
        Status::Continue
    }

    fn handle_end_array(&mut self, _ctx: &Context) -> Status {
        Status::Continue
    }
}

pub fn guess_array(json: &str) -> Result<(String, String)> {
    let mut handler = GuessArray::new();
    let mut parser = Parser::new(&mut handler);
    let string = json.to_string();
    let mut reader = BufReader::new(string.as_bytes());

    if let Err(error) = parser.parse(&mut reader) {
        return Err(Error::YajlishError {
            error: error.to_string(),
        });
    }
    let mut guess = handler.guess.to_string();
    let mut path_guess = handler.path_guess.to_string();

    if guess.is_empty() {
        if !handler.path_guess.is_empty() {
            guess = "list_in_object".to_string();
        }
    } else {
        path_guess = "".to_string();
    }

    Ok((guess, path_guess))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guess_assert(json: &str, guess: &str, path_guess: &str) {
        println!("{}", json);
        assert_eq!(
            guess_array(json).unwrap(),
            (guess.to_string(), path_guess.to_string())
        );
    }

    #[test]
    fn guess() {
        guess_assert(r#"{"moo": "poo"} {"moo": "poo"}"#, "stream", "");
        guess_assert(r#"{"moo": "poo"}"#, "", "");
        guess_assert(r#"{"moo": []}"#, "", "");
        guess_assert(r#"{"moo": [{}]}"#, "list_in_object", "moo");
        guess_assert(r#"{"moo": ["moo", {}]}"#, "", "");
        guess_assert(r#"{"moo": {"moo2": [{}]}}"#, "list_in_object", "moo/moo2");
        guess_assert(r#"[]"#, "top_array", "");
        guess_assert(r#"{"moo": [{"a": "b"}]} {"moo": "poo"}"#, "stream", "");
        guess_assert(r#"{"moo": [{"a": "b" "#, "list_in_object", "moo");
    }
}

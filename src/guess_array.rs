use jiter::{Jiter, Peek};
use snafu::Snafu;

#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error guessing array location: {}", error))]
    GuessError { error: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Best-effort guess of where the records are in a JSON document, for
/// suggesting `--path`. Returns `(guess, path)`:
///   - `("top_array", "")`        — a top-level array
///   - `("stream", "")`           — concatenated top-level values (json-stream)
///   - `("list_in_object", path)` — an object holding an array-of-objects at `path`
///   - `("", "")`                 — none of the above
///
/// Tolerates truncated input: it only needs to reach the first array-of-objects.
pub fn guess_array(json: &str) -> Result<(String, String)> {
    // A top-level array.
    if json.bytes().find(|b| !b.is_ascii_whitespace()) == Some(b'[') {
        return Ok(("top_array".to_string(), String::new()));
    }

    // Two or more complete top-level values => json-stream. A truncated single
    // value yields one (failed) item, which is not a stream.
    let mut complete = 0;
    for value in serde_json::Deserializer::from_str(json).into_iter::<serde::de::IgnoredAny>() {
        if value.is_err() {
            break;
        }
        complete += 1;
        if complete >= 2 {
            return Ok(("stream".to_string(), String::new()));
        }
    }

    // Otherwise, the path of the first array-of-objects within the first value.
    let mut jiter = Jiter::new(json.as_bytes());
    let mut path: Vec<String> = vec![];
    let mut found: Option<String> = None;
    // Errors (including truncation) stop the walk; a path found beforehand stands.
    let _ = find_object_array(&mut jiter, &mut path, &mut found);

    match found {
        Some(path) => Ok(("list_in_object".to_string(), path)),
        None => Ok((String::new(), String::new())),
    }
}

/// Walk a value; set `found` to the slash-joined path of the first array whose
/// first element is an object, then stop.
fn find_object_array(
    jiter: &mut Jiter,
    path: &mut Vec<String>,
    found: &mut Option<String>,
) -> jiter::JiterResult<()> {
    if found.is_some() {
        return Ok(());
    }
    let peek = jiter.peek()?;
    if peek == Peek::Object {
        let mut key = jiter.known_object()?.map(str::to_string);
        while let Some(k) = key {
            path.push(k);
            find_object_array(jiter, path, found)?;
            path.pop();
            if found.is_some() {
                return Ok(());
            }
            key = jiter.next_key()?.map(str::to_string);
        }
        Ok(())
    } else if peek == Peek::Array {
        match jiter.known_array()? {
            Some(elem) if elem == Peek::Object => {
                *found = Some(path.join("/"));
                Ok(())
            }
            Some(elem) => {
                // First element is not an object: skip the whole array.
                jiter.known_skip(elem)?;
                while let Some(element) = jiter.array_step()? {
                    jiter.known_skip(element)?;
                }
                Ok(())
            }
            None => Ok(()), // empty array
        }
    } else {
        jiter.known_skip(peek)?;
        Ok(())
    }
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

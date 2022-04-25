use indexmap::IndexMap as HashMap;
use jsonref::JsonRef;
use serde_json::Value;
use slug::slugify;
use snafu::{ResultExt, Snafu};

#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not deref schema {}: {}", schema, source))]
    FlattererJSONRefError {
        schema: String,
        source: jsonref::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, PartialEq)]
pub struct SchemaAnalysis {
    schema: String,
    path_separator: String,
    pub field_order_map: HashMap<String, usize>,
    field_order: Vec<String>,
    pub field_titles_map: HashMap<String, String>,
    title_tactic: String,
}

impl SchemaAnalysis {
    fn new(schema: &str, path_separator: &str, title_tactic: String) -> SchemaAnalysis {
        SchemaAnalysis {
            schema: schema.to_owned(),
            path_separator: path_separator.to_owned(),
            field_order: vec![],
            field_order_map: HashMap::new(),
            field_titles_map: HashMap::new(),
            title_tactic,
        }
    }

    fn parse(&mut self) -> Result<()> {
        let mut jsonref = JsonRef::new();
        jsonref.set_reference_key("___ref___");
        let value: Value;
        if self.schema.starts_with("http") {
            value = jsonref
                .deref_url(&self.schema)
                .context(FlattererJSONRefSnafu {
                    schema: &self.schema,
                })?;
        } else {
            value = jsonref
                .deref_file(&self.schema)
                .context(FlattererJSONRefSnafu {
                    schema: &self.schema,
                })?;
        }

        self.parse_value(value);

        for (num, item) in self.field_order.iter().enumerate() {
            self.field_order_map.insert(item.clone(), num + 1);
        }

        Ok(())
    }

    fn parse_value(&mut self, schema: Value) {
        if let Some(object) = schema.get("properties") {
            self.parse_properties(object, vec![]);
        }
    }

    fn parse_properties(&mut self, properties: &Value, path: Vec<String>) {
        if let Some(obj) = properties.as_object() {
            for (name, property) in obj {
                let mut new_path = path.clone();
                new_path.push(name.clone());

                if let Some(properties) = property.get("properties") {
                    self.parse_properties(properties, new_path.clone());
                } else if let Some(properties) = property.pointer("/items/properties") {
                    self.parse_properties(properties, new_path.clone());
                } else {
                    let field_path = new_path.join(&self.path_separator);
                    self.field_order.push(field_path.clone());
                    if !self.title_tactic.is_empty() {
                        let mut title = "".to_string();

                        if let Some(title_value) = property.get("title") {
                            if let Some(found_title) = title_value.as_str() {
                                title = found_title.to_owned();
                            };
                        }

                        let title_to_use = match self.title_tactic.as_str() {
                            "underscore_slug" => slugify(title).replace("-", "_"),
                            "slug" => slugify(title),
                            _ => title.to_owned(),
                        };
                        self.field_titles_map
                            .insert(field_path.clone(), title_to_use);
                    }
                }
            }
        }
    }
}

pub fn schema_analysis(
    schema_path: &str,
    path_separator: &str,
    title_tactic: String,
) -> Result<SchemaAnalysis> {
    let mut schema = SchemaAnalysis::new(schema_path, path_separator, title_tactic);
    schema.parse()?;

    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_order() {
        insta::assert_yaml_snapshot!(
             schema_analysis(
                "https://gist.githubusercontent.com/kindly/91e09f88ced65aaca1a15d85a56a28f9/raw/52f8477435cff0b73c54aacc70926c101ce6c685/base.json",
                "_",
                "".to_string(),
                ).unwrap().field_order_map
            )
    }

    #[test]
    fn test_schema_titles() {
        insta::assert_yaml_snapshot!(
            schema_analysis(
                "https://gist.githubusercontent.com/kindly/91e09f88ced65aaca1a15d85a56a28f9/raw/52f8477435cff0b73c54aacc70926c101ce6c685/base.json",
                "_",
                "full".to_string(), 
                ).unwrap().field_titles_map
            );
    }

    #[test]
    fn test_slug() {
        insta::assert_yaml_snapshot!(
            schema_analysis(
                "https://gist.githubusercontent.com/kindly/91e09f88ced65aaca1a15d85a56a28f9/raw/52f8477435cff0b73c54aacc70926c101ce6c685/base.json",
                "_",
                "underscore_slug".to_string(), 
                ).unwrap().field_titles_map
            );

        insta::assert_yaml_snapshot!(
            schema_analysis(
                "https://gist.githubusercontent.com/kindly/91e09f88ced65aaca1a15d85a56a28f9/raw/52f8477435cff0b73c54aacc70926c101ce6c685/base.json",
                "_",
                "slug".to_string(), 
                ).unwrap().field_titles_map
            );
    }
}

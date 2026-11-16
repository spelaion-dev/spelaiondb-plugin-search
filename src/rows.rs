//! First-batch column types stay fixed. Late fields and values of a different type live in
//! the final JSON column, so no hit silently loses data.
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use serde_json::{Map, Value, json};
use spelaion_driver_api::{ColumnMeta, DriverError, ExecEvent, LogicalType, Result, ResultSchema};
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Int,
    Float,
    Bool,
    Text,
    Json,
}
fn kind(v: &Value) -> Kind {
    match v {
        Value::Bool(_) => Kind::Bool,
        Value::Number(n) if n.is_i64() => Kind::Int,
        Value::Number(n) if n.is_f64() => Kind::Float,
        Value::String(_) => Kind::Text,
        _ => Kind::Json,
    }
}
fn documents(engine: &str, answer: Value) -> Vec<Value> {
    if matches!(engine, "elasticsearch" | "opensearch")
        && let Some(hits) = answer["hits"]["hits"].as_array()
    {
        let mut out: Vec<Value> = hits
            .iter()
            .map(|hit| {
                let mut row = Map::new();
                for key in ["_index", "_id", "_score"] {
                    row.insert(key.into(), hit[key].clone());
                }
                if let Some(source) = hit["_source"].as_object() {
                    for (key, value) in source {
                        if !row.contains_key(key) {
                            row.insert(key.clone(), value.clone());
                        } else {
                            row.insert(format!("_source.{key}"), value.clone());
                        }
                    }
                }
                for (key, value) in hit.as_object().into_iter().flatten() {
                    if !matches!(key.as_str(), "_index" | "_id" | "_score" | "_source") {
                        row.insert(key.clone(), value.clone());
                    }
                }
                if let Some(aggs) = answer.get("aggregations") {
                    row.insert("aggregations".into(), aggs.clone());
                }
                Value::Object(row)
            })
            .collect();
        if out.is_empty()
            && let Some(aggs) = answer.get("aggregations")
        {
            out.push(json!({"aggregations":aggs}));
        }
        return out;
    }
    if let Some(docs) = answer["response"]["docs"]
        .as_array()
        .or_else(|| answer["hits"].as_array())
    {
        return docs.clone();
    }
    match answer {
        Value::Array(rows) => rows,
        other => vec![other],
    }
}

pub fn events(engine: &str, answer: Value, batch_size: usize) -> Result<Vec<ExecEvent>> {
    let docs = documents(engine, answer);
    let mut names = Vec::new();
    if matches!(engine, "elasticsearch" | "opensearch") {
        names.extend(["_index", "_id", "_score"].into_iter().map(str::to_string));
    }
    for row in docs.iter().take(batch_size) {
        if let Some(object) = row.as_object() {
            for key in object.keys() {
                if key != "…" && names.len() < 256 && !names.contains(key) {
                    names.push(key.clone());
                }
            }
        } else if names.is_empty() {
            names.push("value".into());
        }
    }
    let value = |row: &Value, name: &str| {
        if row.is_object() {
            row.get(name).cloned().unwrap_or(Value::Null)
        } else if name == "value" {
            row.clone()
        } else {
            Value::Null
        }
    };
    let kinds: Vec<Kind> = names
        .iter()
        .map(|name| {
            let mut values = docs
                .iter()
                .take(batch_size)
                .map(|r| value(r, name))
                .filter(|v| !v.is_null());
            let first = values.next().as_ref().map(kind).unwrap_or(Kind::Json);
            if values.all(|v| kind(&v) == first) {
                first
            } else {
                Kind::Json
            }
        })
        .collect();
    let mut columns: Vec<_> = names
        .iter()
        .zip(&kinds)
        .map(|(name, kind)| ColumnMeta {
            name: name.clone(),
            logical: match kind {
                Kind::Int => LogicalType::Int,
                Kind::Float => LogicalType::Float,
                Kind::Bool => LogicalType::Bool,
                Kind::Text => LogicalType::Text,
                Kind::Json => LogicalType::Json,
            },
            native_type: match kind {
                Kind::Int => "integer",
                Kind::Float => "number",
                Kind::Bool => "boolean",
                Kind::Text => "string",
                Kind::Json => "json",
            }
            .into(),
            native_id: None,
            origin: None,
        })
        .collect();
    columns.push(ColumnMeta {
        name: "…".into(),
        logical: LogicalType::Json,
        native_type: "json".into(),
        native_id: None,
        origin: None,
    });
    let schema = Arc::new(ResultSchema::new(columns));
    let mut events = vec![ExecEvent::Columns(schema.clone())];
    for chunk in docs.chunks(batch_size.max(1)) {
        let mut arrays: Vec<ArrayRef> = Vec::new();
        let mut leftovers: Vec<Map<String, Value>> = chunk
            .iter()
            .map(|row| row.as_object().cloned().unwrap_or_default())
            .collect();
        for (name, k) in names.iter().zip(&kinds) {
            let values: Vec<_> = chunk
                .iter()
                .enumerate()
                .map(|(i, row)| {
                    let v = value(row, name);
                    if v.is_null() || *k == Kind::Json || kind(&v) == *k {
                        leftovers[i].remove(name);
                        v
                    } else {
                        Value::Null
                    }
                })
                .collect();
            arrays.push(match k {
                Kind::Int => Arc::new(Int64Array::from(
                    values.iter().map(Value::as_i64).collect::<Vec<_>>(),
                )),
                Kind::Float => Arc::new(Float64Array::from(
                    values.iter().map(Value::as_f64).collect::<Vec<_>>(),
                )),
                Kind::Bool => Arc::new(BooleanArray::from(
                    values.iter().map(Value::as_bool).collect::<Vec<_>>(),
                )),
                Kind::Text => Arc::new(StringArray::from(
                    values.iter().map(Value::as_str).collect::<Vec<_>>(),
                )),
                Kind::Json => Arc::new(StringArray::from(
                    values
                        .iter()
                        .map(|v| (!v.is_null()).then(|| v.to_string()))
                        .collect::<Vec<_>>(),
                )),
            });
        }
        arrays.push(Arc::new(StringArray::from(
            leftovers
                .into_iter()
                .map(|m| Value::Object(m).to_string())
                .collect::<Vec<_>>(),
        )));
        let batch = RecordBatch::try_new(schema.arrow.clone(), arrays)
            .map_err(|e| DriverError::Other(e.to_string()))?;
        events.push(ExecEvent::Batch(batch));
    }
    events.push(ExecEvent::Done {
        rows_affected: None,
    });
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn late_fields_and_changed_types_survive_in_the_overflow_column() {
        let events = events(
            "meilisearch",
            json!({"hits":[{"id":1},{"id":"second","late":true}]}),
            1,
        )
        .unwrap();
        let ExecEvent::Batch(batch) = &events[2] else {
            panic!("second batch")
        };
        assert!(batch.column(0).is_null(0));
        let overflow = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(overflow.value(0)).unwrap(),
            json!({"id":"second","late":true})
        );
    }
}

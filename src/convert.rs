extern crate csv;

use serde_json::{Deserializer, Value};
use std::collections::{HashMap, HashSet};
use std::error::Error;

// TODO: move code in main here

// TODO: implement flatten and unwind
//
fn flatten_record() {}

fn unwind_record() {}

pub fn convert_header_to_csv_string(headers: &Vec<&String>) -> Result<String, Box<Error>> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    let mut record = Vec::new();
    for item in headers {
        record.push(item.clone());
    }
    wtr.write_record(record)?;
    let data = String::from_utf8(wtr.into_inner()?)?;
    Ok(data)
}

pub fn convert_json_record_to_csv_string(
    headers: &Vec<&String>,
    json_map: &HashMap<String, Value>,
) -> Result<String, Box<Error>> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    // iterate over headers
    // if header is present in record, add it
    // if not, blank string
    let mut record = Vec::new();
    for item in headers {
        let value = json_map.get(&item.to_string());
        let csv_result = match value {
            Some(header_item) => match header_item.as_str() {
                Some(s) => String::from(s),
                None => header_item.to_string(),
            },
            None => String::from(""),
        };
        record.push(csv_result)
    }
    wtr.write_record(record)?;
    let data = String::from_utf8(wtr.into_inner()?)?;
    return Ok(data);
}

// TODO: add tests

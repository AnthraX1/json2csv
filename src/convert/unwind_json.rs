use serde_json::Value;

/// Takes a JSON value and "unwinds" it, based roughly on the behavior of
/// https://docs.mongodb.com/manual/reference/operator/aggregation/unwind/
///
/// Select a key; for each item in that key's array, return a new element
/// with the value of that key replaced by the array item.
///
/// If the key is missing or the value is not an array, the original value
/// is returned unchanged.
pub fn unwind_json(wound_json: Value, unwind_on: &str) -> Vec<Value> {
    let sub_array = match wound_json.get(unwind_on).and_then(|v| v.as_array()) {
        Some(arr) => arr.clone(),
        None => return vec![wound_json],
    };
    let mut base = match wound_json.as_object() {
        Some(obj) => obj.clone(),
        None => return vec![wound_json],
    };
    base.remove(unwind_on);
    sub_array
        .into_iter()
        .map(|item| {
            let mut new_json = base.clone();
            new_json.insert(unwind_on.to_string(), item);
            Value::from(new_json)
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn simple_test() {
        assert_eq!(
            unwind_json(json!({"a": 1, "b": [1,2]}), "b"),
            *json!([{"a": 1, "b": 1}, {"a": 1, "b": 2}])
                .as_array()
                .unwrap()
        )
    }

    #[test]
    fn missing_key_passes_through() {
        let input = json!({"a": 1, "b": 2});
        assert_eq!(unwind_json(input.clone(), "missing"), vec![input]);
    }

    #[test]
    fn non_array_passes_through() {
        let input = json!({"a": 1, "b": "not an array"});
        assert_eq!(unwind_json(input.clone(), "b"), vec![input]);
    }
}

//! Pull-based native metrics; collection is explicitly enabled by the caller.
use crate::error::Failure;
use lancedb::metrics_otel::{self, MetricValue};
use serde_json::json;
use std::sync::OnceLock;

static ENABLED: OnceLock<bool> = OnceLock::new();

#[rustler::nif]
fn metrics_enable() -> Result<bool, Failure> {
    if *ENABLED.get_or_init(metrics_otel::register_metrics_recorder) {
        Ok(true)
    } else {
        Err("Another native metrics recorder is already installed".into())
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn metrics_catalog() -> String {
    let values: Vec<_> = metrics_otel::metrics_catalog().into_iter().map(|m| json!({
        "name": m.name, "kind": m.kind.as_str(), "unit": m.unit, "description": m.description,
    })).collect();
    json!(values).to_string()
}

#[rustler::nif(schedule = "DirtyCpu")]
fn metrics_snapshot() -> String {
    let values: Vec<_> = metrics_otel::snapshot_metrics().into_iter().map(|m| {
        let value = match m.value {
            MetricValue::Scalar(value) => json!({"value": value}),
            MetricValue::Histogram { buckets, count, sum } => json!({"buckets": buckets, "count": count, "sum": sum}),
        };
        json!({"name": m.name, "kind": m.kind.as_str(), "attributes": m.attributes, "measurements": value})
    }).collect();
    json!(values).to_string()
}

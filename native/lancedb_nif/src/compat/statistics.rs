//! Adapters for metadata contracts not represented in LanceDB's typed API.

/// Lance 13 reports IVF loss per entry in "indices". The high-level LanceDB
/// 0.40.0 statistics omit it. Never return a partial or non-finite total.
pub(crate) fn training_loss(stats: &serde_json::Value) -> Option<f64> {
    stats
        .get("indices")?
        .as_array()
        .filter(|segments| !segments.is_empty())?
        .iter()
        .try_fold(0.0, |sum, segment| {
            segment.get("loss")?.as_f64().map(|loss| sum + loss)
        })
        .filter(|loss| loss.is_finite())
}

#[cfg(test)]
mod tests {
    use super::training_loss;
    use serde_json::json;

    #[test]
    fn loss_is_complete_and_additive() {
        assert_eq!(
            training_loss(&json!({"indices": [{"loss": 1.25}, {"loss": 2.5}]})),
            Some(3.75)
        );
        assert_eq!(
            training_loss(&json!({"indices": [{"loss": 0.0}]})),
            Some(0.0)
        );
        for stats in [
            json!({}),
            json!({"indices": []}),
            json!({"indices": null}),
            json!({"indices": [{"loss": 1.0}, {}]}),
            json!({"indices": [{"loss": null}]}),
            json!({"indices": [{"loss": "invalid"}]}),
            json!({"indices": [{"loss": 1.7e308}, {"loss": 1.7e308}]}),
        ] {
            assert_eq!(training_loss(&stats), None);
        }
    }
}

//! Shallow cloning and the pinned SDK's clone/open and URI compatibility rules.
use crate::error::Failure;
use crate::resources::{ConnectionResource, TableResource, RUNTIME};
use crate::table_open::OpenOptions;
use lance::dataset::refs::Ref;
use rustler::ResourceArc;
use std::sync::Arc;

#[rustler::nif(schedule = "DirtyCpu")]
fn clone_table(
    conn: ResourceArc<ConnectionResource>,
    name: String,
    source_uri: String,
    version: Option<u64>,
    tag: Option<String>,
) -> Result<ResourceArc<TableResource>, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        let target_uri = clone_uri(conn.conn.uri(), conn.config.uri(), &name)?;
        let source_ref = match (version, tag) {
            (Some(version), None) => Ref::Version(None, Some(version)),
            (None, Some(tag)) => Ref::Tag(tag),
            (None, None) => Ref::Version(None, None),
            _ => return Err("Cannot specify both source_version and source_tag".into()),
        };
        // LanceDB 0.37.1's clone builder drops storage options when reopening
        // the committed clone. Clone through Lance, then use the connection's
        // ordinary open builder so endpoints, credentials and sessions survive.
        let source = OpenOptions::at(source_uri).open(&conn, &name).await?;
        let wrapper = source
            .dataset()
            .ok_or("Cloning requires an embedded table")?;
        let mut dataset = (*wrapper.get().await.map_err(Failure::from_error)?).clone();
        dataset
            .shallow_clone(&target_uri, source_ref, None)
            .await
            .map_err(Failure::from_error)?;
        let table = OpenOptions::default().open(&conn, &name).await?;
        Ok(ResourceArc::new(TableResource {
            table: Arc::new(table),
        }))
    })
}

// Match ListingDatabase::table_uri, including its normalized engine scheme
// and propagation of query parameters that belong to Lance rather than LanceDB.
fn clone_uri(base: &str, original: &str, name: &str) -> Result<String, Failure> {
    lancedb::utils::validate_table_name(name).map_err(Failure::from_error)?;
    let mut uri = base.to_owned();
    if !uri.ends_with(['/', '\\']) {
        uri.push(if uri.contains("://") {
            '/'
        } else {
            std::path::MAIN_SEPARATOR
        });
    }
    uri.push_str(&format!("{name}.lance"));
    if let Ok(url) = url::Url::parse(original) {
        let query = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(
                url.query_pairs()
                    .filter(|(key, _)| key != "engine" && key != "mirroredStore"),
            )
            .finish();
        if !query.is_empty() {
            uri.push('?');
            uri.push_str(&query);
        }
    }
    Ok(uri)
}

#[cfg(test)]
mod tests {
    use super::clone_uri;

    #[test]
    fn clone_locations_preserve_storage_paths_and_query_parameters() {
        assert_eq!(
            clone_uri("/tmp/db", "/tmp/db", "copy").unwrap(),
            format!("/tmp/db{}copy.lance", std::path::MAIN_SEPARATOR)
        );
        assert_eq!(
            clone_uri("s3://bucket/db/", "s3://bucket/db/", "copy").unwrap(),
            "s3://bucket/db/copy.lance"
        );
        assert_eq!(
            clone_uri(
                "s3+ddb://bucket/db",
                "s3://bucket/db?engine=ddb&ddbTableName=commits",
                "copy"
            )
            .unwrap(),
            "s3+ddb://bucket/db/copy.lance?ddbTableName=commits"
        );
        assert_eq!(
            clone_uri(
                "file:///tmp/db",
                "file:///tmp/db?mirroredStore=cache",
                "copy"
            )
            .unwrap(),
            "file:///tmp/db/copy.lance"
        );
        assert!(clone_uri("/tmp/db", "/tmp/db", "../escape").is_err());
    }
}

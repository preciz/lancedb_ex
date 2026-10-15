//! Shallow cloning and the pinned SDK's clone/open and URI compatibility rules.
use crate::error::Failure;
use crate::resources::{ConnectionResource, TableResource};
use crate::table_open::OpenOptions;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, WriteDestination};
use rustler::{Atom, Env, ResourceArc, Term};
use std::sync::Arc;

#[rustler::nif]
fn clone_table(
    env: Env<'_>,
    conn: ResourceArc<ConnectionResource>,
    name: String,
    source_uri: String,
    version: Option<u64>,
    tag: Option<String>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;

        let table = execute(&conn, &name, source_uri, version, tag).await?;
        Ok(ResourceArc::new(TableResource {
            table: Arc::new(table),
        }))
    })
}

async fn execute(
    conn: &ConnectionResource,
    name: &str,
    source_uri: String,
    version: Option<u64>,
    tag: Option<String>,
) -> Result<lancedb::Table, Failure> {
    let target_uri = clone_uri(conn.conn.uri(), conn.config.uri(), name)?;
    if version.is_some() && tag.is_some() {
        return Err("Cannot specify both source_version and source_tag".into());
    }
    // LanceDB 0.40.0's clone builder drops storage options when reopening the
    // committed clone, and Lance 13.0.0's Dataset::shallow_clone forces the
    // source dataset's ObjectStore onto the destination commit. Commit with
    // source_store so CommitBuilder resolves the destination store from
    // target_uri, then reopen through the connection's ordinary open builder
    // so endpoints, credentials and sessions survive.
    let source = OpenOptions::at(source_uri).open(conn, name).await?;
    let wrapper = source
        .dataset()
        .ok_or("Cloning requires an embedded table")?;
    let dataset = wrapper.get().await.map_err(Failure::from_error)?;
    let (ref_name, ref_version) = match (version, tag) {
        (Some(version), None) => (dataset.manifest().branch.clone(), version),
        (None, Some(tag)) => {
            let contents = dataset
                .tags()
                .get(&tag)
                .await
                .map_err(Failure::from_error)?;
            (contents.branch, contents.version)
        }
        (None, None) => (dataset.manifest().branch.clone(), dataset.version_id()),
        (Some(_), Some(_)) => unreachable!(),
    };
    let source_location = dataset
        .branch_location()
        .find_branch(ref_name.as_deref())
        .map_err(Failure::from_error)?;
    let source_store = dataset
        .object_store(None)
        .await
        .map_err(Failure::from_error)?;
    let transaction = Transaction::new(
        ref_version,
        Operation::Clone {
            is_shallow: true,
            ref_name,
            ref_version,
            ref_path: source_location.uri,
            branch_name: None,
        },
        None,
    );
    CommitBuilder::new(WriteDestination::Uri(&target_uri))
        .with_store_params(dataset.store_params().cloned().unwrap_or_default())
        .with_source_store(source_store)
        .with_session(dataset.session())
        .execute(transaction)
        .await
        .map_err(Failure::write_error)?;
    OpenOptions::default()
        .open(conn, name)
        .await
        .map_err(Failure::write_error)
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
    use super::{clone_uri, execute};
    use crate::connection::ConnectionConfig;
    use crate::resources::{ConnectionResource, RUNTIME};
    use arrow_array::{Int64Array, RecordBatch};
    use std::collections::HashMap;
    use std::sync::Arc;

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

    #[test]
    fn shallow_clone_commits_to_destination_bucket_store() {
        let suffix = format!(
            "{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let source_db = format!("shared-memory://source-{suffix}/db");
        let target_db = format!("shared-memory://target-{suffix}/db");
        RUNTIME.block_on(async {
            let source_config = ConnectionConfig::new(source_db, HashMap::new(), Some(0));
            let source_conn = source_config.connect(None).await.unwrap();
            let batch =
                RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![10, 20])) as _)])
                    .unwrap();
            let source_table = source_conn
                .create_table("docs", batch)
                .execute()
                .await
                .unwrap();
            let source_uri = source_table
                .dataset()
                .unwrap()
                .get()
                .await
                .unwrap()
                .uri()
                .to_owned();

            let target_config = ConnectionConfig::new(target_db, HashMap::new(), Some(0));
            let target_conn = target_config.connect(None).await.unwrap();
            let target_resource = ConnectionResource {
                conn: target_conn.clone(),
                config: target_config,
            };

            let cloned = execute(&target_resource, "copy", source_uri, None, None)
                .await
                .unwrap();
            assert_eq!(cloned.count_rows(None).await.unwrap(), 2);
            assert_eq!(
                source_conn.table_names().execute().await.unwrap(),
                vec!["docs".to_string()]
            );
            assert_eq!(
                target_conn.table_names().execute().await.unwrap(),
                vec!["copy".to_string()]
            );

            let more =
                RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![30])) as _)])
                    .unwrap();
            cloned.add(more).execute().await.unwrap();
            assert_eq!(cloned.count_rows(None).await.unwrap(), 3);
            assert_eq!(source_table.count_rows(None).await.unwrap(), 2);
        });
    }
}

use crate::analyzer::{fts_builder, FtsAnalyzer};
use crate::error::Failure;
use crate::resources::{atoms, TableResource};
use lance::index::DatasetIndexExt;
use lancedb::DistanceType;
use rustler::{Atom, Env, ResourceArc, Term};

/// One bounded readiness observation. The BEAM caller owns polling and sleeps;
/// no native admission slot or dirty scheduler is occupied between observations.
#[rustler::nif(schedule = "DirtyCpu")]
fn index_readiness(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    names: Vec<String>,
    timeout_ms: u64,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    if names.is_empty() || timeout_ms == 0 || timeout_ms > 7_200_000 {
        return Err("Index names and a positive timeout of at most two hours are required".into());
    }
    let deadline = crate::execution::ExecutionOptions {
        timeout_ms: Some(timeout_ms as u32),
    }
    .deadline();
    crate::read_task::spawn(env, reference, deadline, async move {
        let indices = table
            .table
            .list_indices()
            .await
            .map_err(Failure::from_error)?;
        let mut pending = Vec::new();
        for name in names {
            if indices.iter().any(|index| index.name == name) {
                let stats = table
                    .table
                    .index_stats(&name)
                    .await
                    .map_err(Failure::from_error)?;
                if stats.is_some_and(|stats| stats.num_unindexed_rows == 0) {
                    continue;
                }
            }
            pending.push(name);
        }
        Ok(pending)
    })
}

#[derive(serde::Deserialize)]
struct IndexOptions {
    r#type: String,
    name: Option<String>,
    replace: bool,
    train: bool,
    distance: String,
    num_partitions: Option<u32>,
    num_sub_vectors: Option<u32>,
    sample_rate: Option<u32>,
    max_iterations: Option<u32>,
    target_partition_size: Option<u32>,
    num_bits: Option<u32>,
    m: Option<u32>,
    ef_construction: Option<u32>,
    analyzer: Option<FtsAnalyzer>,
    #[serde(default)]
    fts: crate::index_fts::FtsBuildOptions,
}

fn index_builder(
    table: &lancedb::Table,
    column: String,
    options: String,
) -> Result<lancedb::index::IndexBuilder, Failure> {
    use lancedb::index::{
        scalar::{
            BTreeIndexBuilder, BitmapIndexBuilder, FmIndexBuilder, LabelListIndexBuilder,
            ZoneMapIndexBuilder,
        },
        vector::{
            IvfFlatIndexBuilder, IvfHnswFlatIndexBuilder, IvfHnswPqIndexBuilder,
            IvfHnswSqIndexBuilder, IvfPqIndexBuilder, IvfRqIndexBuilder, IvfSqIndexBuilder,
        },
        Index,
    };
    let opts: IndexOptions = serde_json::from_str(&options).map_err(Failure::from_error)?;
    let distance = match opts.distance.as_str() {
        "l2" => DistanceType::L2,
        "cosine" => DistanceType::Cosine,
        "dot" => DistanceType::Dot,
        "hamming" => DistanceType::Hamming,
        _ => return Err("Invalid distance".into()),
    };
    // SDK builder types have matching setters but no common trait. Apply IVF
    // training controls once here, then each family's quantization/graph options.
    macro_rules! vector_index {
        ($variant:ident, $builder:ty $(, $option:ident => $setter:ident)*) => {{
            let mut builder = <$builder>::default().distance_type(distance);
            if let Some(value) = opts.num_partitions {
                builder = builder.num_partitions(value);
            }
            if let Some(value) = opts.sample_rate {
                builder = builder.sample_rate(value);
            }
            if let Some(value) = opts.max_iterations {
                builder = builder.max_iterations(value);
            }
            if let Some(value) = opts.target_partition_size {
                builder = builder.target_partition_size(value);
            }
            $(if let Some(value) = opts.$option {
                builder = builder.$setter(value);
            })*
            Index::$variant(builder)
        }};
    }
    let index = match opts.r#type.as_str() {
        "auto" => Index::Auto,
        "fts" => Index::FTS(opts.fts.apply(fts_builder(opts.analyzer)?)?),
        "btree" => Index::BTree(BTreeIndexBuilder::default()),
        "bitmap" => Index::Bitmap(BitmapIndexBuilder::default()),
        "label_list" => Index::LabelList(LabelListIndexBuilder::default()),
        "fm" => Index::Fm(FmIndexBuilder::default()),
        "zonemap" => Index::ZoneMap(ZoneMapIndexBuilder::default()),
        "ivf_flat" => vector_index!(IvfFlat, IvfFlatIndexBuilder),
        "ivf_sq" => vector_index!(IvfSq, IvfSqIndexBuilder),
        "ivf_rq" => vector_index!(IvfRq, IvfRqIndexBuilder, num_bits => num_bits),
        "ivf_pq" => vector_index!(IvfPq, IvfPqIndexBuilder,
            num_sub_vectors => num_sub_vectors, num_bits => num_bits),
        "hnsw_flat" => vector_index!(IvfHnswFlat, IvfHnswFlatIndexBuilder,
            m => num_edges, ef_construction => ef_construction),
        "hnsw_pq" => vector_index!(IvfHnswPq, IvfHnswPqIndexBuilder,
            m => num_edges, ef_construction => ef_construction,
            num_sub_vectors => num_sub_vectors, num_bits => num_bits),
        "hnsw_sq" => vector_index!(IvfHnswSq, IvfHnswSqIndexBuilder,
            m => num_edges, ef_construction => ef_construction),
        _ => return Err("Unsupported index type".into()),
    };
    let mut builder = table
        .create_index(&[column], index)
        .replace(opts.replace)
        .train(opts.train);
    if let Some(name) = opts.name {
        builder = builder.name(name);
    }
    Ok(builder)
}

#[rustler::nif]
fn create_index(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    column: String,
    options: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;
        table.ensure_mutable()?;
        crate::compat::fts::index(index_builder(&table.table, column, options)?.execute())
            .await
            .map_err(Failure::write_error)?;
        Ok(atoms::created())
    })
}

#[rustler::nif]
fn create_index_async(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    column: String,
    options: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        table.ensure_mutable()?;
        let job = crate::compat::fts::index(
            index_builder(&table.table, column, options)?.execute_async(),
        )
        .await
        .map_err(Failure::write_error)?;
        Ok(crate::job::JobResource::new(job, permit))
    })
}

#[rustler::nif]
fn list_indices(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let indices = table_res
            .table
            .list_indices()
            .await
            .map_err(Failure::from_error)?;
        let indices: Vec<_> = indices.into_iter().map(|i| serde_json::json!({
            "name": i.name, "index_type": i.index_type.to_string(), "columns": i.columns,
            "num_indexed_rows": i.num_indexed_rows, "num_unindexed_rows": i.num_unindexed_rows,
            "index_uuid": i.index_uuid, "size_bytes": i.size_bytes,
        })).collect();
        Ok(serde_json::json!(indices).to_string())
    })
}

#[rustler::nif]
fn index_stats(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    name: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let stats = table_res
            .table
            .index_stats(name.clone())
            .await
            .map_err(Failure::from_error)?;
        let loss = if stats.as_ref().is_some_and(|s| s.distance_type.is_some()) {
            if let Some(dataset) = table_res.table.dataset() {
                let dataset = dataset.get().await.map_err(Failure::from_error)?;
                let raw = dataset
                    .index_statistics(&name)
                    .await
                    .map_err(Failure::from_error)?;
                let raw: serde_json::Value =
                    serde_json::from_str(&raw).map_err(Failure::from_error)?;
                crate::compat::training_loss(&raw)
            } else {
                None
            }
        } else {
            None
        };
        Ok(match stats {
            None => "null".into(),
            Some(s) => serde_json::json!({
                "num_indexed_rows": s.num_indexed_rows,
                "num_unindexed_rows": s.num_unindexed_rows,
                "index_type": s.index_type.to_string(),
                "distance_type": s.distance_type.map(|d| d.to_string()),
                "num_indices": s.num_indices,
                "loss": loss,
            })
            .to_string(),
        })
    })
}

#[rustler::nif]
fn drop_index(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    name: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;
        table_res.ensure_mutable()?;
        table_res
            .table
            .drop_index(&name)
            .await
            .map_err(Failure::write_error)?;
        Ok(atoms::dropped())
    })
}

#[rustler::nif]
fn prewarm_index(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    name: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        table_res
            .table
            .prewarm_index(&name)
            .await
            .map_err(Failure::from_error)?;
        Ok(atoms::prewarmed())
    })
}

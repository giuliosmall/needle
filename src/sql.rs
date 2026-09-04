//! DataFusion SQL over RAP point-lookup rows.
//!
//! After a key lookup, the decoded listens are registered as table `hits`.
//! `needle_lookup(key)` is a table function that runs another RAP lookup.

use crate::index::load_index_for_keys;
use crate::query::{ListenRow, QueryOptions, RapQuerier};
use anyhow::{Context, Result};
use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::datasource::memory::MemTable;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct SqlOptions {
    pub index: PathBuf,
    pub key: Option<String>,
    pub sql: String,
    pub query: QueryOptions,
}

pub struct SqlResult {
    pub batch: RecordBatch,
    pub key: Option<String>,
}

/// Schema of `hits` / `needle_lookup` rows (ListenRow columns).
pub fn listen_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("track_uri", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Int64, false),
        Field::new("source_file", DataType::Utf8, false),
        Field::new("row_number", DataType::UInt64, false),
    ]))
}

pub fn listen_rows_to_batch(rows: &[ListenRow]) -> Result<RecordBatch> {
    let user_id = StringArray::from_iter_values(rows.iter().map(|r| r.user_id.as_str()));
    let timestamp_ms = Int64Array::from_iter_values(rows.iter().map(|r| r.timestamp_ms));
    let track_uri = StringArray::from_iter_values(rows.iter().map(|r| r.track_uri.as_str()));
    let duration_ms = Int64Array::from_iter_values(rows.iter().map(|r| r.duration_ms));
    let source_file = StringArray::from_iter_values(rows.iter().map(|r| r.source_file.as_str()));
    let row_number = UInt64Array::from_iter_values(rows.iter().map(|r| r.row_number));
    Ok(RecordBatch::try_new(
        listen_schema(),
        vec![
            Arc::new(user_id),
            Arc::new(timestamp_ms),
            Arc::new(track_uri),
            Arc::new(duration_ms),
            Arc::new(source_file),
            Arc::new(row_number),
        ],
    )?)
}

fn lookup_hits(index: &Path, key: &str, query: &QueryOptions) -> Result<RecordBatch> {
    let idx = load_index_for_keys(index, &[key.to_string()])?;
    let querier = RapQuerier::new(idx);
    let res = querier.query_with(key, query)?;
    listen_rows_to_batch(&res.rows)
}

/// Convenience wrapper: RAP lookup for `key`, then SQL against `hits`.
pub fn sql_lookup(index: impl AsRef<Path>, key: &str, sql: &str) -> Result<SqlResult> {
    run_sql(&SqlOptions {
        index: index.as_ref().to_path_buf(),
        key: Some(key.to_string()),
        sql: sql.to_string(),
        query: QueryOptions::default(),
    })
}

/// Block on a tokio runtime. Fetch key via RapQuerier if key is Some, register `hits`, run SQL.
pub fn run_sql(opts: &SqlOptions) -> Result<SqlResult> {
    let hits = if let Some(key) = &opts.key {
        lookup_hits(&opts.index, key, &opts.query)?
    } else {
        RecordBatch::new_empty(listen_schema())
    };

    let sql = opts.sql.clone();
    let index = opts.index.clone();
    let query = opts.query.clone();

    let rt = tokio::runtime::Runtime::new()?;
    let batch = rt.block_on(async move { exec_sql(hits, &sql, index, query).await })?;
    Ok(SqlResult {
        batch,
        key: opts.key.clone(),
    })
}

async fn exec_sql(
    hits: RecordBatch,
    sql: &str,
    index: PathBuf,
    query: QueryOptions,
) -> Result<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_batch("hits", hits)
        .context("register table hits")?;
    ctx.register_udtf(
        "needle_lookup",
        Arc::new(NeedleLookup { index, query }),
    );
    let df = ctx
        .sql(sql)
        .await
        .with_context(|| format!("plan sql: {sql}"))?;
    let schema = df.schema().inner().clone();
    let batches = df.collect().await.with_context(|| format!("exec sql: {sql}"))?;
    Ok(concat_batches(&schema, &batches)?)
}

#[derive(Debug, Clone)]
struct NeedleLookup {
    index: PathBuf,
    query: QueryOptions,
}

impl TableFunctionImpl for NeedleLookup {
    fn call_with_args(
        &self,
        args: TableFunctionArgs<'_, '_>,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let key = literal_key(args.exprs())?;
        let batch = lookup_hits(&self.index, &key, &self.query)
            .map_err(|e| DataFusionError::Execution(format!("{e:#}")))?;
        Ok(Arc::new(MemTable::try_new(
            batch.schema(),
            vec![vec![batch]],
        )?))
    }
}

fn literal_key(exprs: &[Expr]) -> datafusion::common::Result<String> {
    let Some(expr) = exprs.first() else {
        return plan_err!("needle_lookup(key) requires a string key");
    };
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Ok(s.clone()),
        other => plan_err!("needle_lookup expects a string literal, got {other}"),
    }
}

/// Pretty-print batch as JSON array of objects (for CLI).
pub fn batch_to_json(batch: &RecordBatch) -> Result<Vec<serde_json::Value>> {
    let mut out = Vec::with_capacity(batch.num_rows());
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    for row in 0..batch.num_rows() {
        let mut obj = serde_json::Map::with_capacity(names.len());
        for (i, name) in names.iter().enumerate() {
            obj.insert(name.clone(), cell_json(batch.column(i).as_ref(), row)?);
        }
        out.push(serde_json::Value::Object(obj));
    }
    Ok(out)
}

fn cell_json(col: &dyn Array, row: usize) -> Result<serde_json::Value> {
    if col.is_null(row) {
        return Ok(serde_json::Value::Null);
    }
    let any = col.as_any();
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return Ok(serde_json::Value::String(a.value(row).to_string()));
    }
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return Ok(serde_json::json!(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<Int32Array>() {
        return Ok(serde_json::json!(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<UInt64Array>() {
        return Ok(serde_json::json!(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<UInt32Array>() {
        return Ok(serde_json::json!(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        return Ok(serde_json::json!(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<Float32Array>() {
        return Ok(serde_json::json!(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<BooleanArray>() {
        return Ok(serde_json::json!(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<TimestampMillisecondArray>() {
        return Ok(serde_json::json!(a.value(row)));
    }
    Ok(serde_json::Value::String(format!(
        "{:?}",
        col.data_type()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{IndexBuilder, load_index};
    use crate::writer::{WriteMode, WriterOptions, write_sample_dataset};

    fn setup() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("parquet");
        let idx = tmp.path().join("rap-index");
        let opts = WriterOptions {
            out_dir: data,
            num_users: 12,
            listens_per_user: 4,
            num_files: 2,
            mode: WriteMode::Sorted,
            rows_per_row_group: 32,
            write_page_index: true,
            seed: 123,
            one_page_per_key: false,
        };
        let paths = write_sample_dataset(&opts).unwrap();
        IndexBuilder::new(&idx, 8)
            .with_covering(true)
            .build_fragment(&paths, "frag-sql", None)
            .unwrap();
        (tmp, idx)
    }

    fn i64_named(batch: &RecordBatch, name: &str, row: usize) -> i64 {
        let col = batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column {name} in {:?}", batch.schema()));
        let any = col.as_any();
        if let Some(a) = any.downcast_ref::<Int64Array>() {
            return a.value(row);
        }
        if let Some(a) = any.downcast_ref::<UInt64Array>() {
            return a.value(row) as i64;
        }
        if let Some(a) = any.downcast_ref::<Int32Array>() {
            return a.value(row) as i64;
        }
        panic!("column {name} has unexpected type {:?}", col.data_type());
    }

    #[test]
    fn sql_count_hits_for_user() {
        let (_tmp, idx) = setup();
        let querier = RapQuerier::new(load_index(&idx).unwrap());
        let rap = querier.query("user_0000").unwrap();
        let res = run_sql(&SqlOptions {
            index: idx,
            key: Some("user_0000".into()),
            sql: "SELECT count(*) AS n FROM hits".into(),
            query: QueryOptions::default(),
        })
        .unwrap();
        assert_eq!(res.batch.num_rows(), 1);
        assert_eq!(i64_named(&res.batch, "n", 0), rap.rows.len() as i64);
        assert_eq!(rap.rows.len(), 4);
    }

    #[test]
    fn sql_group_by_track() {
        let (_tmp, idx) = setup();
        let res = sql_lookup(
            &idx,
            "user_0000",
            "SELECT track_uri, count(*) AS n FROM hits GROUP BY track_uri ORDER BY n DESC",
        )
        .unwrap();
        assert!(
            res.batch.num_rows() >= 1,
            "group by track_uri should return at least 1 row"
        );
        let json = batch_to_json(&res.batch).unwrap();
        assert_eq!(json.len(), res.batch.num_rows());
        assert!(json[0].get("track_uri").is_some());
        assert!(json[0].get("n").is_some());
    }
}

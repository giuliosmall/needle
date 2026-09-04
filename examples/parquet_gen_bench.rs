use rap::lake::LakeGenerateOpts;
use std::time::Instant;

fn main() -> anyhow::Result<()> {
    for (n, par) in [(500usize, 32usize), (2000, 64)] {
        let opts = LakeGenerateOpts {
            files: n,
            parallelism: par,
            index_dir: format!("data/bench-idx-{n}").into(),
            fragment_id: "b".into(),
            days: 10,
            hash_buckets: 10,
            clear_index: true,
            rows_per_file: 2,
            ..Default::default()
        };
        rap::lake::minio_up()?;
        let t0 = Instant::now();
        let man = rap::lake::lake_generate(&opts)?;
        println!(
            "n={n} par={par} → {} objs in {:?} ({:.1}/s) bytes={}",
            man.objects,
            t0.elapsed(),
            man.objects as f64 / t0.elapsed().as_secs_f64().max(1e-9),
            man.bytes_uploaded
        );
    }
    Ok(())
}

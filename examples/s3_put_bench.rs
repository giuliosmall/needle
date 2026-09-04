use rap::s3::S3Client;
use std::time::Instant;

fn main() -> anyhow::Result<()> {
    let c = S3Client::from_env();
    let body = vec![b'x'; 2048];
    let n = 200usize;
    let t0 = Instant::now();
    for i in 0..n {
        let key = format!("bench/part-{i:05}.bin");
        c.put_object("rap-lake", &key, &body)?;
    }
    let dt = t0.elapsed();
    println!(
        "puts={n} in {:?} ({:.1}/s) stats={:?}",
        dt,
        n as f64 / dt.as_secs_f64(),
        c.stats.snapshot()
    );
    Ok(())
}

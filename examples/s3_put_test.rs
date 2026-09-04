use needle::s3::S3Client;

fn main() -> anyhow::Result<()> {
    let c = S3Client::from_env();
    let body = b"PAR1-test-payload-for-sigv4";
    c.put_object("rap-lake", "debug/sigv4-test.bin", body)?;
    println!("put ok");
    let got = c.get_object("rap-lake", "debug/sigv4-test.bin")?;
    assert_eq!(got, body);
    println!("get ok {}", got.len());
    let part = c.get_range("rap-lake", "debug/sigv4-test.bin", &(0..4))?;
    assert_eq!(part, &body[0..4]);
    println!("range ok {:?}", part);
    let part2 = c.get_range("rap-lake", "debug/sigv4-test.bin", &(5..11))?;
    println!("range2 ok {:?}", std::str::from_utf8(&part2));
    // key with =
    c.put_object(
        "rap-lake",
        "date=2024-01-01/bucket=000/part-test.parquet",
        body,
    )?;
    let r = c.get_range(
        "rap-lake",
        "date=2024-01-01/bucket=000/part-test.parquet",
        &(0..4),
    )?;
    println!("equals-key range ok {:?}", r);
    Ok(())
}

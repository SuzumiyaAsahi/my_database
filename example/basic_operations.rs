use bytes::Bytes;
use my_data::{db, options::Options};

fn main() -> anyhow::Result<()> {
    let ops = Options::default();
    let engine = db::Engine::open(ops).expect("failed to open bitcask engine");
    engine.put(Bytes::from("name"), Bytes::from("bitcask-rs"))?;

    let _ = engine.get(Bytes::from("name"))?;

    engine.delete(Bytes::from("name"))?;
    Ok(())
}

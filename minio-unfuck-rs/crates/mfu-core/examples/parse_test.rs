use mfu_core::xlmeta;
use std::env;
use std::fs;

fn main() {
    let args: Vec<String> = env::args().collect();
    let path = args.get(1).map(|s| s.as_str()).unwrap_or(".disks/storage1/recordings/test/xl.meta");

    let data = fs::read(path).expect("read file");
    println!("File: {}", path);
    println!("File size: {}", data.len());
    println!("Header bytes: {:02x?}", &data[0..16.min(data.len())]);

    match xlmeta::parse(&data) {
        Ok(meta) => {
            println!("Parsed successfully!");
            println!("  size: {}", meta.size);
            println!("  data_blocks: {}", meta.data_blocks);
            println!("  parity_blocks: {}", meta.parity_blocks);
            println!("  data_dir: {}", meta.data_dir_string());
            println!("  etag: {}", meta.etag);
        }
        Err(e) => {
            println!("Parse failed: {:?}", e);
        }
    }
}

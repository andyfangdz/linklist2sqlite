extern crate csv;
extern crate rusqlite;
extern crate memmap;

use std::fs::OpenOptions;
use std::path::PathBuf;
use memmap::MmapMut;
use std::env;

use std::error::Error;
use std::io;
use std::process;
use std::collections::HashSet;
use std::collections::HashMap;
use csv::StringRecord;

struct GraphMetaData {

}

fn read_edge_list() -> Result<(), Box<Error>> {
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b' ')
        .from_reader(io::stdin());

    let mut nodes = HashSet::new();
    let mut nodes_id_map = HashMap::new();
    let mut id_nodes_map = HashMap::new();

    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result.unwrap().to_owned();
        let src = record.get(0).unwrap().to_owned();
        let dst = record.get(1).unwrap().to_owned();
        if !nodes.contains(src.as_str()) {
            let id = nodes.len() as u32;
            nodes_id_map.insert(src.clone(), id);
            id_nodes_map.insert(id, src.clone());
            nodes.insert(src);
        }
        if !nodes.contains(dst.as_str()) {
            let id = nodes.len() as u32;
            nodes_id_map.insert(dst.clone(), id);
            id_nodes_map.insert(id, dst.clone());
            nodes.insert(dst);
        }
    }
    println!("{:?}", nodes_id_map);
    Ok(())
}

fn main() {
    read_edge_list();
}

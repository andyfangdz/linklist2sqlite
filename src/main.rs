extern crate csv;
extern crate memmap;
extern crate rusqlite;

use csv::StringRecord;
use memmap::MmapMut;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::mem;
use std::path::PathBuf;
use std::process;

use std::fs::File;
use std::io::BufReader;

unsafe fn as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>())
}

struct GraphMetaData {}

#[derive(Debug)]
#[repr(C, packed)]
struct NodeEntry {
    offset: u64,
    length: u64,
}

#[repr(C, packed)]
struct EdgeEntry {
    source: u32,
    target: u32,
}

fn read_edge_list() -> Result<(), Box<Error>> {
    let edge_list_path = PathBuf::from("/home/andy/Downloads/soc-LiveJournal1.txt");

    let f = File::open(edge_list_path)?;
    let edge_list_reader = BufReader::new(f);

    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .comment(Some(b'#'))
        .has_headers(false)
        .from_reader(edge_list_reader);
    let mut nodes_id_map = HashMap::new();
    let mut id_nodes_map = HashMap::new();

    let mut edge_count: u64 = 0;

    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result.unwrap();

        let src = record.get(0).unwrap();
        let dst = record.get(1).unwrap();

        if !nodes_id_map.contains_key(src) {
            let id = nodes_id_map.len() as u32;
            nodes_id_map.insert(src.to_owned(), id);
            id_nodes_map.insert(id, src.to_owned());
        }

        if !nodes_id_map.contains_key(dst) {
            let id = nodes_id_map.len() as u32;
            nodes_id_map.insert(dst.to_owned(), id);
            id_nodes_map.insert(id, dst.to_owned());
        }

        edge_count += 1;
    }

    let node_count = nodes_id_map.len() as u32;

    let nodes_path = PathBuf::from("graph.nodes");
    let nodes_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&nodes_path)?;
    nodes_file.set_len((node_count as usize * mem::size_of::<NodeEntry>()) as u64)?;

    let edges_path = PathBuf::from("graph.edges");
    let edges_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&edges_path)?;
    edges_file.set_len((edge_count as usize * mem::size_of::<EdgeEntry>()) as u64)?;

    let mut nodes_map = unsafe { MmapMut::map_mut(&nodes_file)? };
    let mut edges_map = unsafe { MmapMut::map_mut(&edges_file)? };

    let mut last_node = String::from("");
    let mut cur: u64 = 0;
    let mut cur_len: u64 = 0;
    let mut n_edge: u64 = 0;
    rdr.seek(csv::Position::new());
    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result.unwrap();
        let src = record.get(0).unwrap().to_owned();
        let dst = record.get(1).unwrap().to_owned();

        if last_node == "" {
            last_node = src.clone();
        }

        if src != last_node {
            let n_node = nodes_id_map.get(&last_node).unwrap().clone();
            let node_map_start = n_node as usize * mem::size_of::<NodeEntry>();
            let node_map_end = node_map_start + mem::size_of::<NodeEntry>();
            (&mut nodes_map[node_map_start..node_map_end]).write_all(unsafe {
                as_u8_slice(&NodeEntry {
                    offset: cur - cur_len,
                    length: cur_len,
                })
            });
            last_node = src.clone();
            cur_len = 0;
        }

        let edge_map_start = cur as usize * mem::size_of::<EdgeEntry>();
        let edge_map_end = edge_map_start + mem::size_of::<EdgeEntry>();
        (&mut edges_map[edge_map_start..edge_map_end]).write_all(unsafe {
            as_u8_slice(&EdgeEntry {
                source: nodes_id_map.get(&src).unwrap().clone(),
                target: nodes_id_map.get(&dst).unwrap().clone(),
            })
        });

        cur += 1;
        cur_len += 1;
        n_edge += 1;
    }

    let n_node = nodes_id_map.get(&last_node).unwrap().clone();
    // write last node
    let node_map_start = n_node as usize * mem::size_of::<NodeEntry>();
    let node_map_end = node_map_start + mem::size_of::<NodeEntry>() + 1;
    (&mut nodes_map[node_map_start..node_map_end]).write_all(unsafe {
        as_u8_slice(&NodeEntry {
            offset: cur - cur_len,
            length: cur_len,
        })
    });

    nodes_map.flush();
    edges_map.flush();

    Ok(())
}

fn main() {
    read_edge_list();
}

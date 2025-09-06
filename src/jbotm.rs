// jbloxDB
// © 2025 Green Arrowhead LLP
// Licensed under the jbloxDB License v1.0
// See LICENSE.txt for terms.
// Free for individuals and small companies.
// Commercial license required for production use by companies over USD 5M revenue or for SaaS/product distribution.

use memmap2::{MmapMut, MmapOptions};
use memmap2::{Mmap};
use rayon::prelude::*;
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{self, Seek, SeekFrom, Write, BufWriter};
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;
use serde_json::Value;
use serde::Deserialize;
use config::Config;
use chrono::Local;
use anyhow::Result;
use std::error::Error;
use rustc_hash::FxHashSet;
use sorted_vec::SortedVec;



use std::env;
use std::fs;
use std::path::{Path};
use std::process;

use std::io::{BufRead, BufReader};

use std::io::{Read};

use std::sync::{Arc, Mutex};

use std::thread;
use std::clone::Clone;
use serde_json::{ Number};

use std::collections::{BTreeMap, BTreeSet};
use std::cmp::Ordering;
use std::borrow::Borrow;


use std::ops::Bound::{Included, Excluded, Unbounded};


use std::fmt;
use ordered_float::NotNan; // Cargo.toml: ordered-float = "4"
use std::sync::{RwLock};
use std::borrow::Cow;
use std::io::{ErrorKind};


use std::sync::{MutexGuard};

use std::sync::{RwLockReadGuard, RwLockWriteGuard};

use indexmap::{IndexMap, indexmap};

type Ts  = String;
type Ptr = usize;

static RECORD_SIZE_DIGITCOUNT: usize = 7;
static RECORDKEY_SIZE_DIGITCOUNT: usize = 4;
static TIMESTAMP_LEN: usize = 18;
static ADD_TOMARK_REC_DELETE_IN_RECORDKEY: usize = 100_000;


#[derive(Debug, Clone, Copy)]
enum ZeroKind { D, I }

#[cfg(unix)]
unsafe fn discard_pages(ptr: *mut u8, len: usize) {
    let ret = libc::madvise(ptr as *mut libc::c_void, len, libc::MADV_DONTNEED);
    if ret != 0 {
        eprintln!("madvise failed: {}", std::io::Error::last_os_error());
    }
}

#[cfg(windows)]
unsafe fn discard_pages(_ptr: *mut u8, _len: usize) {
    // Windows does not support madvise. No-op.
}

/// Thread-safe wrapper around a raw pointer into a memory-mapped file.
#[derive(Debug)]
pub struct PtrWrapper(pub UnsafeCell<*mut u8>);

impl PtrWrapper {
    pub fn new(ptr: *mut u8) -> Self {
        PtrWrapper(UnsafeCell::new(ptr))
    }

    /// Advance pointer by offset
    pub fn add(&self, offset: usize) -> *mut u8 {
        unsafe { (*self.0.get()).add(offset) }
    }
    /// Raw pointer access
    pub fn as_ptr(&self) -> *mut u8 {
        unsafe { *self.0.get() }
    }
    /// Compute offset from another pointer
    pub fn offset_from(&self, other: &Self) -> isize {
        unsafe { (*self.0.get()).offset_from(*other.0.get()) }
    }
    pub fn from_base_and_offset(base: *mut u8, offset: usize) -> Self {
        let ptr = unsafe { base.add(offset) };
        PtrWrapper::new(ptr)
    }
}

impl Clone for PtrWrapper {
    fn clone(&self) -> Self {
        PtrWrapper(UnsafeCell::new(self.as_ptr()))
    }
}
impl PartialEq for PtrWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.as_ptr() == other.as_ptr()
    }
}

impl Eq for PtrWrapper {}
impl std::hash::Hash for PtrWrapper {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (self.as_ptr() as usize).hash(state);
    }
}
unsafe impl Send for PtrWrapper {}
unsafe impl Sync for PtrWrapper {}

//for sorting keys
#[derive(Clone, Debug, Eq, PartialEq)]
struct NumKey(String);

impl Ord for NumKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // Try numeric comparison
        match (self.0.parse::<usize>(), other.0.parse::<usize>()) {
            (Ok(a), Ok(b)) => a.cmp(&b),
            _ => self.0.cmp(&other.0), // fallback to normal string comparison
        }
    }
}

impl PartialOrd for NumKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for NumKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl From<&str> for NumKey {
    fn from(s: &str) -> Self { NumKey(s.to_string()) }
}

impl From<String> for NumKey {
    fn from(s: String) -> Self { NumKey(s) }
}

// optional convenience
impl From<usize> for NumKey {
    fn from(n: usize) -> Self { NumKey(n.to_string()) }
}

// Let lookups by &str work: map.get("10")
impl Borrow<str> for NumKey {
    fn borrow(&self) -> &str { &self.0 }
}

impl fmt::Display for NumKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0) // or write!(f, "{:0width$}", self.0, width = 4)
    }
}

#[derive(Debug, Clone)]
pub struct Stats<K> {
    pub total_n: usize,
    pub unique_n: usize,
    pub min_key: Option<K>,
    pub max_key: Option<K>,
    pub mean: f64,
    pub var_population: f64,
    pub var_sample: Option<f64>, // None if N < 2
    pub std_population: f64,
    pub std_sample: Option<f64>,
    pub median: Option<f64>,
    pub mode_keys: Vec<K>,
    pub mode_count: usize,
}

#[derive(Debug, Deserialize,Clone)]
pub struct Settings {
    initfilesize:String, 
    newfilesizemultiplier:f32, //new file size = this var times existing size
    datadir: String,
    logdir: String,
    logmaxlines: usize,
    indexnamevaluedelimiter: char,
    indexdelimiter: char,
    recorddelimiter: char,
    repindexnamevaluedelimiter: char,
    repindexdelimiter: char,
    reprecorddelimiter: char,
    maxgetrecords: usize,
    maxrecordlength: usize,
    maxlogtoconsolelength: usize,
    enableviewdelete: bool,
    MADVISE_CHUNK: usize,
    low_ram_mode: bool,
    notoperator: String,
    sanitycheckgobacksec: usize,
    debugmode: bool,
    strongdurablemode: bool,

} 
pub struct jbothandler {
    settings: Settings,
    file_size_map: HashMap<String, usize>, //file->file length
    //file_mmap_map: HashMap<String, MmapMut>,//file->mmep of file
    file_line_map: HashMap<String, BTreeMap<usize, usize>>,//file->pointer vector
    file_key_line_map: HashMap<String, BTreeMap<usize, usize>>,//file->pointer vector
    file_key_pointer_map: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>, //file->key name -> key -> pointer
    file_key_pointer_fordeleted_map: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>, //file->key name -> key -> pointer
    datadir: String,
    logdir: String,
    log_file: BufWriter<File>,
    errorlog_file: BufWriter<File>,
    log_line_count : usize,
    state: Arc<Mutex<SharedState>>,
    stateR: Arc<Mutex<SharedStateR>>,
    size_digits: usize,
    size_digits_keys: usize,


}

struct SharedState {
    // per-file lock: each value is Arc<Mutex<MmapMut>>
    file_mmap_map: Arc<RwLock<HashMap<String, Arc<Mutex<MmapMut>>>>>,
}
struct SharedStateR {
    // read maps don’t need a Mutex per-file; keep Arc<Mmap> for cheap cloning
    file_mmap_map_forread: Arc<RwLock<HashMap<String, Arc<Mmap>>>>,
}

// ---------- Types this function expects to exist ----------
// type NumKey = ...;           // your existing key type (Ord)
// struct Settings {            // your existing config type
//     recorddelimiter: String,
//     indexdelimiter: String,
//     indexnamevaluedelimiter: String,
//     initfilesize: usize,
//     newfilesizemultiplier: usize,
//     enableviewdelete: bool,
//     low_ram_mode: bool,
//     MADVISE_CHUNK: usize,
//     maxrecordlength: usize,
// }
// extern "Rust" fn load_existing_file(...)-> Result<usize, anyhow::Error>; // your function

// Returned container (matches your outer map schema)
pub struct ThreadMaps2 {
    pub file_size_map: HashMap<String, usize>,
    pub file_line_map: HashMap<String, BTreeMap<usize, usize>>, // offset -> len
    pub file_key_line_map: HashMap<String, BTreeMap<usize, usize>>,
    pub file_key_pointer_map: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
    pub file_key_pointer_fordeleted_map: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
}

impl ThreadMaps2 {
    fn new() -> Self {
        Self {
            file_size_map: Default::default(),
            file_line_map: Default::default(),
            file_key_line_map: Default::default(),
            file_key_pointer_map: Default::default(),
            file_key_pointer_fordeleted_map: Default::default(),
        }
    }
}
type PtrMap = HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>;

// Small helper: merge one ThreadMaps2 into another (no locking here)
fn merge_tm2(dst: &mut ThreadMaps2, mut src: ThreadMaps2) {
    for (k, v) in src.file_size_map {
        dst.file_size_map.entry(k).or_insert(v);
    }
    for (file, mut m) in src.file_line_map {
        dst.file_line_map.entry(file).or_default().append(&mut m);
    }
    for (file, mut m) in src.file_key_line_map {
        dst.file_key_line_map.entry(file).or_default().append(&mut m);
    }
    // nested pointer maps
    let merge_ptr = |
        dst_h: &mut HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
        mut src_h: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
    | {
        for (file, mut m2) in src_h.drain() {
            let d2 = dst_h.entry(file).or_default();
            for (key, mut m3) in m2.drain() {
                let d3 = d2.entry(key).or_default();
                for (nk, mut v) in m3 {
                    d3.entry(nk).or_default().append(&mut v);
                }
            }
        }
    };
    merge_ptr(&mut dst.file_key_pointer_map, src.file_key_pointer_map);
    merge_ptr(&mut dst.file_key_pointer_fordeleted_map, src.file_key_pointer_fordeleted_map);
}


pub fn parallel_load_ranges_collect(
    path_str: String,
    settings: &Settings,
    size_digits: usize,
    size_digits_keys: usize,
    maxrecord: usize,
    file_mmap_map_thread: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<std::sync::Mutex<memmap2::MmapMut>>>>>,
    file_mmap_map_forread_thread: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<memmap2::Mmap>>>>,
) -> anyhow::Result<ThreadMaps2> {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc};
    use crossbeam_channel as chan;
    use crossbeam::thread as xscope;
    use memmap2::{Mmap};

    // ---------- 1) File size & chunking ----------
    let file_len: usize = std::fs::metadata(&path_str)?.len() as usize;
    let n_threads: usize = std::cmp::max(1, num_cpus::get_physical());
    let chunk: usize = (file_len + n_threads - 1) / n_threads;

    // ---------- 2) Optional RO mmap and header alignment helpers ----------
    let mmap_ro_opt: Option<Arc<Mmap>> = {
        let g = file_mmap_map_forread_thread.read().unwrap();
        g.get(&path_str).cloned()
    };

    // settings.* are chars in your codebase
    let rec_del_byte: u8 = (settings.recorddelimiter as u32) as u8;

    // quick header predicate: [size_digits ASCII digits][recorddelimiter]
    let looks_like_header = |off: usize, mmap: &Mmap| -> bool {
        if off + size_digits >= mmap.len() { return false; }
        let s = &mmap[off..];
        if s.len() < size_digits + 1 { return false; }
        if !s[..size_digits].iter().all(|&b| (b'0'..=b'9').contains(&b)) { return false; }
        s[size_digits] == rec_del_byte
    };

    let align_to_header = |mut off: usize, end: usize, mmap: &Mmap| -> usize {
        while off < end {
            if looks_like_header(off, mmap) { break; }
            off += 1;
        }
        off
    };

    // Build disjoint (start,end) ranges
    let mut ranges = Vec::with_capacity(n_threads);
    for i in 0..n_threads {
        let start = i * chunk;
        let mut end = ((i + 1) * chunk).min(file_len);
        if i == n_threads - 1 { end = file_len; }
        if let Some(ref mmap_ro) = mmap_ro_opt {
            let start_aligned = align_to_header(start, end, mmap_ro);
            if start_aligned < end {
                ranges.push((start_aligned, end));
            }
        } else {
            if start < end {
                ranges.push((start, end));
            }
        }
    }

    // ---------- 3) Local (per-thread) container ----------
    #[derive(Default)]
    struct Local {
        fsz: HashMap<String, usize>,
        flines: HashMap<String, BTreeMap<usize, usize>>,
        fkeyline: HashMap<String, BTreeMap<usize, usize>>,
        fptr: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
        fptr_del: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
    }

    // Cache tiny strings once, share via Arc; pass &str to loader
    let rec_delim_s = Arc::new(settings.recorddelimiter.to_string());
    let idx_delim_s = Arc::new(settings.indexdelimiter.to_string());
    let idx_nv_delim_s = Arc::new(settings.indexnamevaluedelimiter.to_string());

    // ---------- 4) Spawn scoped workers ----------
    let (tx, rx) = chan::unbounded::<Local>();

    xscope::scope(|scope| {
        for (start, end) in ranges {
            let tx = tx.clone();
            let path = path_str.clone();
            let file_mmap_map_thread = file_mmap_map_thread.clone();
            let file_mmap_map_forread_thread = file_mmap_map_forread_thread.clone();

            let rec_delim_s = rec_delim_s.clone();
            let idx_delim_s = idx_delim_s.clone();
            let idx_nv_delim_s = idx_nv_delim_s.clone();

            scope.spawn(move |_| {

                //let tid = std::thread::current().id();
                //println!(
                //    "[DEBUG] Thread {:?} started for range {}..{} on file {}",
                //    tid, start, end, path
                //);

                let mut cur = start;
                let mut loc = Local::default();

                loop {
                    let res = load_existing_file(
                        &settings.initfilesize,
                        settings.newfilesizemultiplier,
                        &path,
                        &*rec_delim_s,
                        &*idx_delim_s,
                        &*idx_nv_delim_s,
                        settings.enableviewdelete,
                        settings.low_ram_mode,
                        settings.MADVISE_CHUNK,
                        size_digits,
                        size_digits_keys,
                        settings.maxrecordlength,
                        maxrecord,
                        cur,
                        &mut loc.fsz,
                        &file_mmap_map_thread,
                        &file_mmap_map_forread_thread,
                        &mut loc.flines,
                        &mut loc.fkeyline,
                        &mut loc.fptr,
                        &mut loc.fptr_del,
                        settings,
                    );

                    match res {
                        Ok(next_off) => {
                            if next_off <= cur { break; }
                            cur = next_off;
                            if cur >= end { break; }
                        }
                        Err(_) => {
                            cur += 1;
                            if cur >= end { break; }
                        }
                    }
                }

                //println!(
                //    "[DEBUG] Thread {:?} finished range {}..{} for file {}",
                //    tid, start, end, path
                //);

                let _ = tx.send(loc);
            });
        }
        drop(tx);
        }).map_err(|e| {
            // turn Box<dyn Any + Send> into anyhow::Error with a useful message
            if let Some(s) = e.downcast_ref::<&str>() {
                anyhow::anyhow!("scoped thread panicked: {}", s)
            } else if let Some(s) = e.downcast_ref::<String>() {
                anyhow::anyhow!("scoped thread panicked: {}", s)
            } else {
                anyhow::anyhow!("scoped thread panicked with non-string payload")
            }
        })?;

    // ---------- 5) Merge all locals into one ThreadMaps2 ----------
    let mut out = ThreadMaps2::new();

    // small mergers
    let mut merge_ptr = |
        dst_h: &mut HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
        mut src_h: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
    | {
        for (file, mut m2) in src_h.drain() {
            let d2 = dst_h.entry(file).or_default();
            for (key, mut m3) in m2.drain() {
                let d3 = d2.entry(key).or_default();
                for (nk, mut v) in m3 {
                    d3.entry(nk).or_default().append(&mut v);
                }
            }
        }
    };

    for loc in rx {
        // sizes
        for (k, v) in loc.fsz { out.file_size_map.entry(k).or_insert(v); }
        // lines
        for (file, mut m) in loc.flines {
            out.file_line_map.entry(file).or_default().append(&mut m);
        }
        // key->line
        for (file, mut m) in loc.fkeyline {
            out.file_key_line_map.entry(file).or_default().append(&mut m);
        }
        // ptr maps
        merge_ptr(&mut out.file_key_pointer_map, loc.fptr);
        merge_ptr(&mut out.file_key_pointer_fordeleted_map, loc.fptr_del);
    }

    Ok(out)
}


    /// Quickly checks if any valid record exists in later half of file.
    /// Returns true if found, false otherwise.
    pub fn records_exist_in_second_half(
        file: &mut File,
        record_prefix: &[u8],
        max_record_length: u64,
    ) -> io::Result<bool> {
        let filesize = file.metadata()?.len();

        if filesize == 0 || filesize <= max_record_length {
            return Ok(false); // File too small to have records in second half
        }

        let midpoint = filesize / 2;

        // Start scanning from midpoint backwards by max_record_length
        let scan_start = midpoint.saturating_sub(max_record_length + 1);
        let scan_len = (filesize - scan_start).min(max_record_length * 2); // Small scan window
        let mut buf = vec![0u8; scan_len as usize];

        file.seek(SeekFrom::Start(scan_start))?;
        let bytes_read = file.read(&mut buf)?;

        if bytes_read == 0 {
            return Ok(false);
        }

        // Scan buffer for valid record prefix
        for i in 0..(bytes_read - record_prefix.len()) {
            if buf[i..i + record_prefix.len()] == *record_prefix {
                return Ok(true); // Found a valid record
            }
        }

        Ok(false) // No valid records found in later half
    }

    
    pub fn get_config_path() -> PathBuf {
        let mut current_dir = match env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(Path::to_path_buf))
        {
            Some(dir) => dir,
            None => {
                eprintln!("Error: Cannot determine current executable path.");
                process::exit(1);
            }
        };

        loop {
            let config_path = current_dir.join("config/jbloxsettings.toml");
            if config_path.exists() {
                return config_path;
            }

            // Try accessing parent
            match fs::metadata(&current_dir) {
                Ok(_) => {
                    if !current_dir.pop() {
                        break; // Reached root
                    }
                }
                Err(_) => {
                    eprintln!("Error: Access denied or unreadable directory: {}", current_dir.display());
                    break;
                }
            }
        }

        eprintln!("Error: Could not find 'config/settings.toml' in current or any parent directory.");
        process::exit(1);
    }



    /// Extracts and parses the embedded JSON after the second `:` in `rec2`.
    /// Returns the parsed `Value` on success, or `Value::Null` on any error.
    pub fn extract_json(rec2: &str,recorddelimt: char) -> Value {
        // Split on the first two ':' characters
        let mut parts = rec2.splitn(3, recorddelimt); //":"
        let _prefix = parts.next();            // "00-2807…000506"
        let _meta   = parts.next();            // "hostkey-HOST1234"
        let json_snip = parts.next().unwrap_or("").trim_end_matches(';');
        
        // Parse that snippet directly as JSON
        serde_json::from_str(json_snip).unwrap_or(Value::Null)
    }


    /// Recursively merge `patch` into `master` in‑place.
    /// Special case: if `patch` is a string of the form "+=<n>", and `master` is a number,
    /// then add `<n>` to the original number instead of overwriting.
    pub fn merge_in_place(master: &mut Value, patch: &Value, skip: &HashSet<&str>) {
        match (master, patch) {
            // Object ↔ Object: merge per key; honor skip at any depth
            (Value::Object(m), Value::Object(p)) => {
                for (k, pv) in p {
                    if skip.contains(k.as_str()) {
                        // do not touch this field anywhere in the tree
                        continue;
                    }
                    match m.get_mut(k) {
                        Some(mv) => merge_in_place(mv, pv, skip),
                        None => {
                            // only insert if not skipped (already checked)
                            m.insert(k.clone(), pv.clone());
                        }
                    }
                }
            }

            // Array ↔ Array: merge element-wise, then append extras
            (Value::Array(ma), Value::Array(pa)) => {
                let common = ma.len().min(pa.len());
                for i in 0..common {
                    merge_in_place(&mut ma[i], &pa[i], skip);
                }
                if pa.len() > ma.len() {
                    ma.extend(pa[common..].iter().cloned());
                }
            }

            // All other combinations: apply "+=" numeric addition if requested; else overwrite
            (m_slot, p_slot) => {
                if let Value::String(s) = p_slot {
                    if let Some(rest) = s.strip_prefix("+=") {
                        match m_slot {
                            // Case 1: already a number
                            Value::Number(orig_num) => {
                                // integer add
                                if let (Some(oi), Ok(ai)) = (orig_num.as_i64(), rest.parse::<i64>()) {
                                    *m_slot = Value::Number(Number::from(oi + ai));
                                    return;
                                }
                                // float add
                                if let (Some(of), Ok(af)) = (orig_num.as_f64(), rest.parse::<f64>()) {
                                    *m_slot = Value::from(of + af);
                                    return;
                                }
                            }

                            // Case 2: numeric string — try to parse it
                            Value::String(orig_str) => {
                                if let (Ok(oi), Ok(ai)) = (orig_str.parse::<i64>(), rest.parse::<i64>()) {
                                    *m_slot = Value::Number(Number::from(oi + ai));
                                    return;
                                }
                                if let (Ok(of), Ok(af)) = (orig_str.parse::<f64>(), rest.parse::<f64>()) {
                                    *m_slot = Value::from(of + af);
                                    return;
                                }
                            }

                            _ => {}
                        }
                    }else if let Some(rest) = s.strip_prefix("*=") {
                        match m_slot {
                            // Case 1: already a number
                            Value::Number(orig_num) => {
                                if let (Some(oi), Ok(ai)) = (orig_num.as_i64(), rest.parse::<i64>()) {
                                    *m_slot = Value::Number(Number::from(oi * ai));
                                    return;
                                }
                                if let (Some(of), Ok(af)) = (orig_num.as_f64(), rest.parse::<f64>()) {
                                    *m_slot = Value::from(of * af);
                                    return;
                                }
                            }
                            // Case 2: numeric string
                            Value::String(orig_str) => {
                                if let (Ok(oi), Ok(ai)) = (orig_str.parse::<i64>(), rest.parse::<i64>()) {
                                    *m_slot = Value::Number(Number::from(oi * ai));
                                    return;
                                }
                                if let (Ok(of), Ok(af)) = (orig_str.parse::<f64>(), rest.parse::<f64>()) {
                                    *m_slot = Value::from(of * af);
                                    return;
                                }
                            }
                            _ => {}
                        }
                    }                    
                }
                // fallback overwrite
                *m_slot = p_slot.clone();
            }
        }
    }


    /// Convenience wrapper: returns a fresh merged `Value` without mutating inputs.
    pub fn merge(master: &Value, patch: &Value, skip: &HashSet<&str>) -> Value {
        let mut result = master.clone();
        merge_in_place(&mut result, patch, skip);
        result
    }   



/// spec_or_line: either just the RHS like "1;customer:3;user:40"
///               or the full "initfilesize = 1;customer:3;user:40"
/// file: the file/key to look up, e.g. "user", "customer"
/// this will enable admins to configure customezed file size for different stores
pub fn value_with_overrides(spec_or_line: &str, file: &str) -> usize {
    // Accept either "name = spec" or just "spec"
    let rhs = spec_or_line.split_once('=').map(|(_, rhs)| rhs).unwrap_or(spec_or_line);

    // tokens are like: ["1", "customer:3", "user:40"]
    let mut toks = rhs.split(';').map(|s| s.trim()).filter(|s| !s.is_empty());

    // default value (if missing or non-numeric → 0)
    let default_val = toks
        .next()
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0);

    // scan overrides: "key:value"
    for tok in toks {
        if let Some((k, vstr)) = tok.split_once(':') {
            if k.trim() == file {
                if let Ok(v) = vstr.trim().parse::<usize>() {
                    return v;
                }
            }
        }
    }

    // no matching override → default
    default_val
}  
/////////////////////////////////////////////////////

pub fn load_existing_file(
    initfilesize: &str,
    filesizemultiplier: f32, // for future use
    filepath: &str,
    recorddelimiter: &str,
    indexdelimiter: &str,
    indexnamevaluedelimiter: &str,
    enableviewdelete: bool,
    low_ram_mode: bool,
    MADVISE_CHUNK_S: usize,
    size_digits: usize,
    size_digits_keys: usize,
    maxrecordlength: usize,
    maxrecords: usize,
    inputoffset: usize,
    // plain maps you already lock before calling
    file_size_map: &mut HashMap<String, usize>,
    file_mmap_map: &Arc<RwLock<HashMap<String, Arc<Mutex<memmap2::MmapMut>>>>>,   // ★ per-file lock
    file_mmap_map_forread: &Arc<RwLock<HashMap<String, Arc<memmap2::Mmap>>>>,     // ★ shared read
    file_line_map: &mut HashMap<String, BTreeMap<usize, usize>>,
    file_key_line_map: &mut HashMap<String, BTreeMap<usize, usize>>,
    file_key_pointer_map: &mut HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
    file_key_pointer_fordeleted_map: &mut HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
    settings: &Settings,
) -> io::Result<usize> {



    let path = Path::new(filepath);
    let mut offset: usize = inputoffset;

    if path.extension().and_then(|e| e.to_str()) != Some("jblox") {
        return Ok(offset);
    }

    let fname = path.file_stem().unwrap().to_string_lossy().to_string();
    
    //let tid = std::thread::current().id();
    //println!("[ENTER]  {:?} load_existing_file file={} inputoffset={}", tid, fname, inputoffset);

    // Open file read/write and ensure minimum size
    let mut file = OpenOptions::new().read(true).write(true).create(true).open(&path)?;
    let filemetadata = file.metadata()?;
    let mut current_file_size = filemetadata.len() as usize;

    let configuredfilesize = value_with_overrides(&initfilesize, &fname);
    let initfilesizebytes = configuredfilesize * 1064 * 1064; // (kept as in your code)
    if current_file_size < initfilesizebytes {
        file.set_len(initfilesizebytes as u64)?;
        current_file_size = initfilesizebytes;
    }

    // Clone for read-only mapping
    let fileread = file.try_clone()?;

    // Create a read-only mmap and register it (Arc<Mmap>)
    let mmapread_arc = {
        let mut w 
                = lock_rw_write(file_mmap_map_forread, "file_mmap_map_forread.write()",settings.debugmode);
        w.entry(fname.clone())
            .or_insert_with(|| {
                let newmap = unsafe { MmapOptions::new().map(&fileread).unwrap() };
                Arc::new(newmap)
            })
            .clone()
    };

    // Ensure a per-file write mmap exists (Arc<Mutex<MmapMut>>) — lazy init
    let write_mmap_arc: Arc<Mutex<memmap2::MmapMut>> = {
        let mut w 
                = lock_rw_write(file_mmap_map, "file_mmap_map.write()",settings.debugmode);
        if let Some(existing) = w.get(&fname) {
            // already present: just clone the Arc so this call can use it
            existing.clone()
        } else {
            // create ONLY when absent
            let mmap_mut = unsafe { memmap2::MmapOptions::new()
                .len(current_file_size)
                .map_mut(&file)? };
            let arc = Arc::new(Mutex::new(mmap_mut));
            w.insert(fname.clone(), arc.clone());
            arc
        }
    };

    // We’ll use the read-only mapping to parse bytes below.
    let mmap = &*mmapread_arc;

    // Prepare per-file line maps
    let lines = file_line_map.entry(fname.clone()).or_insert_with(BTreeMap::new);
    let key_lines = file_key_line_map.entry(fname.clone()).or_insert_with(BTreeMap::new);

    let keymap = file_key_pointer_map
        .entry(fname.clone())
        .or_insert_with(HashMap::new);
    let keymap_fordeleted = file_key_pointer_fordeleted_map
        .entry(fname.clone())
        .or_insert_with(HashMap::new);

    // (Kept: delimiter → u16, if you need it later)
    let _delim_u16 = if recorddelimiter.as_bytes().len() >= 2 {
        u16::from_le_bytes(recorddelimiter.as_bytes()[0..2].try_into().unwrap())
    } else {
        u16::from_le_bytes([recorddelimiter.as_bytes()[0], 0])
    };

    // Parse loop (same logic, but reads from `mmap` (Arc<Mmap>))
    let mut whilecount: usize = 0;
    let mut lastoffset = 0;
    {
        while offset < current_file_size {
            if whilecount == 0 {
                whilecount = 1;
            } else if whilecount >= 1 {
                if lastoffset == offset {
                    break;
                } else {
                    lastoffset = offset;
                }
            }

            let deletemark_start = offset;
            let deletemark_end = deletemark_start + 2 * (size_digits * 2); // first 2 numbers → delete/not
            let rec_size_start = offset + ((3 + TIMESTAMP_LEN + 1) * 2);
            let rec_size_end = rec_size_start + (size_digits * 2);

            if rec_size_end > current_file_size {
                break;
            }

            let reckey_size_start = rec_size_end + (1 * 2);
            let reckey_size_end = reckey_size_start + (size_digits_keys * 2);
            if reckey_size_end > current_file_size {
                break;
            }

            // deleted mark check (first byte)
            let mut is_zero = false;
            let deletedmark_slice = &mmap[deletemark_start..deletemark_end];
            if let Some(&first_byte) = deletedmark_slice.first() {
                match first_byte {
                    b'0' => is_zero = true,
                    b'1'..=b'9' => { /* is_zero stays false */ }
                    _ => break, // not a digit
                }
            }

            // record length (UTF-16)
            let size_slice = &mmap[rec_size_start..rec_size_end];
            let size_utf16: Vec<u16> = size_slice
                .chunks(2)
                .map(|b| u16::from_le_bytes([b[0], b[1]]))
                .collect();
            let size_str = String::from_utf16(&size_utf16).unwrap_or_else(|_| "0".to_string());
            let record_len: usize = size_str.trim().parse::<usize>().unwrap_or(0);

            if !(record_len > 0) {
                // recover by scanning to next newline (UTF-16)
                if let Some(pos) = mmap[offset..]
                    .chunks_exact(2)
                    .position(|pair| pair == [b'\n', 0x00])
                {
                    let newline_abs = offset + (pos + 1) * 2;
                    offset = newline_abs;
                    println!("Warning: jblox file : {} contains invalid data", fname);
                    continue;
                }
            }

            // key-record length (UTF-16)
            let key_size_slice = &mmap[reckey_size_start..reckey_size_end];
            let key_size_utf16: Vec<u16> = key_size_slice
                .chunks(2)
                .map(|b| u16::from_le_bytes([b[0], b[1]]))
                .collect();
            let key_size_str =
                String::from_utf16(&key_size_utf16).unwrap_or_else(|_| "0".to_string());
            let key_record_len: usize = key_size_str.trim().parse::<usize>().unwrap_or(0);

            // pointer (you were using offset)
            let recptr = offset;

            lines.insert(recptr, record_len);
            //deleted records have ADD_TOMARK_REC_DELETE_IN_RECORDKEY added
            //to key length
            if(!is_zero){
                key_lines.insert(recptr, key_record_len + ADD_TOMARK_REC_DELETE_IN_RECORDKEY);
            }
            else{
                key_lines.insert(recptr, key_record_len);
            }            

            // key field text (UTF-16)
            let keystr_start = reckey_size_end + 1 * 2;
            let keystr_end = keystr_start + key_record_len;
            if keystr_end > current_file_size {
                break;
            }
            let keystr_slice = &mmap[keystr_start..keystr_end];
            let keystr_utf16: Vec<u16> = keystr_slice
                .chunks(2)
                .map(|b| u16::from_le_bytes([b[0], b[1]]))
                .collect();

            if let Ok(key_field) = String::from_utf16(&keystr_utf16) {
                for part in key_field.split(indexdelimiter) {
                    if let Some(dash_pos) = part.find(indexnamevaluedelimiter) {
                        let key_name = &part[..dash_pos];
                        let key_value = &part[dash_pos + 1..];

                        // NOTE: Keeping your original branch semantics intact.
                        if is_zero {
                            keymap
                                .entry(key_name.to_string())
                                .or_default()
                                .entry(NumKey(key_value.into()))
                                .or_default()
                                .push(recptr);
                        } else {
                            keymap_fordeleted
                                .entry(key_name.to_string())
                                .or_default()
                                .entry(NumKey(key_value.into()))
                                .or_default()
                                .push(recptr);
                        }
                    }
                }
            }

            offset += record_len;
            // Update size map
            file_size_map.insert(fname.clone(), offset);        
        }

    }



    //println!("[LEAVE]  {:?} load_existing_file file={} final_offset={}", tid, fname, offset);

    Ok(offset)
}

/// Lock a `Mutex<T>`. In debugmode, print contention/wait timing; otherwise just block.
fn lock_mutex<'a, T>(m: &'a Mutex<T>, where_: &str, debugmode: bool) -> MutexGuard<'a, T> {
    if !debugmode {
        // Don’t panic on poisoning; log and recover the inner guard.
        return match m.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                eprintln!("[WARN] mutex poisoned at {where_}; continuing with inner guard");
                poisoned.into_inner()
            }
        };
    }
    let tid = thread::current().id();
    let t0 = Instant::now();
    loop {
        if let Ok(g) = m.try_lock() {
            println!("[LOCKED  MUTEX] {:?} at {} after {:?}", tid, where_, t0.elapsed());
            return g;
        } else {
            // throttle to avoid spam; tweak as needed
            if t0.elapsed().as_millis() % 200 == 0 {
                println!("[CONTEND MUTEX] {:?} waiting at {}", tid, where_);
            }
            thread::yield_now();
        }
    }
}

/// Lock a `RwLock<T>` for WRITE. If `debugmode` is true, print contention/wait timing.
fn lock_rw_write<'a, T>(rw: &'a RwLock<T>, where_: &str, debugmode: bool) -> RwLockWriteGuard<'a, T> {
    if !debugmode {
        return match rw.write() {
            Ok(g) => g,
            Err(poisoned) => {
                eprintln!("[WARN] rwlock poisoned at {where_}; continuing with inner guard");
                poisoned.into_inner()
            }
        };
    }
    let tid = thread::current().id();
    let t0 = Instant::now();
    loop {
        if let Ok(g) = rw.try_write() {
            println!("[LOCKED   WLOCK] {:?} at {} after {:?}", tid, where_, t0.elapsed());
            return g;
        } else if t0.elapsed().as_millis() % 200 == 0 {
            println!("[CONTEND  WLOCK] {:?} waiting at {}", tid, where_);
        }
        thread::yield_now();
    }
}

/// Lock a `RwLock<T>` for READ. If `debugmode` is true, print contention/wait timing.
fn lock_rw_read<'a, T>(rw: &'a RwLock<T>, where_: &str, debugmode: bool) -> RwLockReadGuard<'a, T> {
    if !debugmode {
        // Non-debug: no panic on poison; log and recover the inner guard.
        return match rw.read() {
            Ok(g) => g,
            Err(poisoned) => {
                eprintln!("[WARN] rwlock poisoned at {where_}; continuing with inner guard");
                poisoned.into_inner()
            }
        };
    }
    let tid = thread::current().id();
    let t0 = Instant::now();
    loop {
        if let Ok(g) = rw.try_read() {
            println!("[LOCKED   RLOCK] {:?} at {} after {:?}", tid, where_, t0.elapsed());
            return g;
        } else if t0.elapsed().as_millis() % 200 == 0 {
            println!("[CONTEND  RLOCK] {:?} waiting at {}", tid, where_);
        }
        thread::yield_now();
    }
}
////////////////////////////////////////////////////////////


impl jbothandler {


pub fn new() -> io::Result<Self> {
    //get config file path
    let config_path = get_config_path();
    println!("Config file: {}", config_path.to_str().unwrap());
    let config = Config::builder()
        .add_source(config::File::with_name(config_path.to_str().unwrap()))
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config build error: {}", e)))?;

    let settings: Settings = config
        .try_deserialize()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config deserialize error: {}", e)))?;

    //directory to be used to store data
    let datadir = settings.datadir.clone();
    //log directory
    let logdir = settings.logdir.clone();
    println!("log dir: {}", logdir);
    println!("datadir dir: {}", datadir);

    //
    //log frequency in hours settings.logmaxlines;
    //indexnamevalue delimiter
    let indexnamevaluedelimiter: char = settings.indexnamevaluedelimiter.clone().to_string().chars().next().unwrap_or('-'); //'-'
    //index delimiter
    let indexdelimiter: char = settings.indexdelimiter.clone().to_string().chars().next().unwrap_or('`');//'`'
    //record delimiter
    let recorddelimiter: char = settings.recorddelimiter.clone().to_string().chars().next().unwrap_or(':');//':'

    let repindexnamevaluedelimiter: char = settings.recorddelimiter.clone().to_string().chars().next().unwrap_or('_');
    let repindexdelimiter: char = settings.recorddelimiter.clone().to_string().chars().next().unwrap_or('_');
    let reprecorddelimiter: char = settings.recorddelimiter.clone().to_string().chars().next().unwrap_or('_');

    //set size_digits
    let size_digits = RECORD_SIZE_DIGITCOUNT; // Hard coded, to save space. limits size of each record to ~ 10MB
    let size_digits_keys = RECORDKEY_SIZE_DIGITCOUNT;

    let mut log_line_count: usize = 0;

    let log_filename = format!("{}/jblox.log", logdir);

    //everytime jbloxdb start, new error file will be created
    let errlog_filename = format!(
        "{}/jblox_error-{}.log",
        logdir,
        Local::now().format("%Y%m%d-%H%M%S") // e.g., 20250903-002530
    );
    
    println!("log_filename: {}", log_filename);
    let log_file = BufWriter::new(
        OpenOptions::new().create(true).append(true).open(&log_filename)?
    );
    
    let errorlog_file = BufWriter::new(
        OpenOptions::new().create(true).append(true).open(&errlog_filename)?
    );    
    let log_path = std::path::Path::new(&log_filename);
    if log_path.exists() {
        if let Ok(logfileforlines) = File::open(log_path) {
            let reader = BufReader::new(logfileforlines);
            log_line_count = reader.lines().count();
        }
    }

    // --- Own these maps here (plain) ---
    let mut file_size_map: HashMap<String, usize> = HashMap::new();
    let mut file_line_map: HashMap<String, BTreeMap<usize, usize>> = HashMap::new();
    let mut file_key_line_map: HashMap<String, BTreeMap<usize, usize>> = HashMap::new();
    let mut file_key_pointer_map: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>> = HashMap::new();
    let mut file_key_pointer_fordeleted_map: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>> = HashMap::new();

    // --- Per-file locking maps (shared handles) ---
    let file_mmap_map: Arc<RwLock<HashMap<String, Arc<Mutex<MmapMut>>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let file_mmap_map_forread: Arc<RwLock<HashMap<String, Arc<Mmap>>>> =
        Arc::new(RwLock::new(HashMap::new()));

    for entry in std::fs::read_dir(datadir.clone())? {
        let mut currentoffset: usize = 0;
        let maxrecord: usize = 200_000;
 
        let path = entry?.path();
        let path_str = path.to_string_lossy().to_string();

        if !(path.extension().and_then(|ext| ext.to_str()) == Some("jblox")) {
            continue;
        }


        // Debug: announce parallel start for this file
        //println!(
        //    "[DEBUG] parallel load starting: file={}, threads={}",
        //    path_str,
        //    std::cmp::max(1, num_cpus::get_physical())
        //);

        // Run the parallel collector
        let tm = parallel_load_ranges_collect(
            path_str.clone(),
            &settings,
            size_digits,
            size_digits_keys,
            maxrecord,
            Arc::clone(&file_mmap_map),
            Arc::clone(&file_mmap_map_forread),
        ).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Merge results into your plain HashMaps
        for (k, v) in tm.file_size_map {
            file_size_map.entry(k).or_insert(v);
        }
        for (file, mut m) in tm.file_line_map {
            file_line_map.entry(file).or_default().append(&mut m);
        }
        for (file, mut m) in tm.file_key_line_map {
            file_key_line_map.entry(file).or_default().append(&mut m);
        }
        for (file, m2) in tm.file_key_pointer_map {
            let d2 = file_key_pointer_map.entry(file).or_default();
            for (key, m3) in m2 {
                let d3 = d2.entry(key).or_default();
                for (nk, mut v) in m3 {
                    d3.entry(nk).or_default().append(&mut v);
                }
            }
        }
        for (file, m2) in tm.file_key_pointer_fordeleted_map {
            let d2 = file_key_pointer_fordeleted_map.entry(file).or_default();
            for (key, m3) in m2 {
                let d3 = d2.entry(key).or_default();
                for (nk, mut v) in m3 {
                    d3.entry(nk).or_default().append(&mut v);
                }
            }
        }

        // Debug: announce parallel end for this file
        //println!("[DEBUG] parallel load finished: file={}", path_str);

        let mut file_name_str = "";
        if let Some(file_name_os) = path.file_stem() {
            if let Some(s) = file_name_os.to_str() {
                file_name_str = s;
            } else {
                println!("Non-UTF8 file name, using fallback.");
                file_name_str = "UNKNOWN";
            }
        } else {
            println!("Skipping path with no filename: {:?}", path);
        }

        if let Some(lines) = file_line_map.get(file_name_str) {
            println!("File: {}; Total Number of records: {}", file_name_str, lines.len());
        } else {
            println!("No entry for {}", file_name_str);
        }
    }

    // Keep shared RwLock handles in state structs (per-file locking available to rest of app)
    let state = Arc::new(Mutex::new(SharedState {
        file_mmap_map: Arc::clone(&file_mmap_map),
    }));
    let stateR = Arc::new(Mutex::new(SharedStateR {
        file_mmap_map_forread: Arc::clone(&file_mmap_map_forread),
    }));


    Ok(Self {
        settings,
        file_size_map,
        file_line_map,
        file_key_line_map,
        file_key_pointer_map,
        file_key_pointer_fordeleted_map,
        datadir,
        logdir,
        log_file,
        errorlog_file,
        log_line_count,
        state,
        stateR,
        size_digits,
        size_digits_keys,
    })
}


    /// Weighted mean over f64 values parsed from NumKey.
    /// Weight for each key = vec.len() (number of records/pointers).
    /// Skips keys that fail to parse or aren't finite.
    fn mean_from_btreemapptr(&self, map: &BTreeMap<NumKey, Vec<usize>>) -> f64 {
        let mut sum_w = 0.0f64;  // total weight
        let mut mean  = 0.0f64;  // running weighted mean

        for (k, v) in map {
            let w = v.len() as f64;
            if w == 0.0 { continue; }

            let x = match k.0.parse::<f64>() {
                Ok(val) if val.is_finite() => val,
                _ => continue, // skip non-numeric / non-finite
            };

            // Weighted online update
            let sum_w_new = sum_w + w;
            let delta = x - mean;
            mean += (w / sum_w_new) * delta;
            sum_w = sum_w_new;
        }

        if sum_w == 0.0 { f64::NAN } else { mean }
    }

    fn mean_from_numkey_slice(&self,v: &[NumKey]) -> f64 {
        if v.is_empty() {
            return f64::NAN;
        }

        let mut mean = 0.0f64;
        let mut n = 0.0f64;

        for nk in v {
            // assuming NumKey.0 is a String (or &str) that can parse to f64
            let x = match nk.0.parse::<f64>() {
                Ok(val) if val.is_finite() => val,
                _ => continue, // skip invalid / non-finite
            };

            n += 1.0;
            mean += (x - mean) / n; // incremental update
        }

        if n == 0.0 { f64::NAN } else { mean }
    }

    /// Mode over float keys parsed from NumKey; ties -> smaller value.
    /// Skips entries that fail to parse or are NaN.
    fn mode_frm_btree_mapptr(&self, map: &BTreeMap<NumKey, Vec<usize>>)
        -> Option<(f64, usize)>
    {
        let mut freq: HashMap<NotNan<f64>, usize> = HashMap::new();

        for (key, vec_ptrs) in map {
            if let Ok(v) = key.0.parse::<f64>() {
                if let Ok(nn) = NotNan::new(v) {
                    *freq.entry(nn).or_insert(0) += vec_ptrs.len();
                } else {
                    // v was NaN; skip (NotNan::new returned Err)
                    // eprintln!("[DEBUG] NaN value for key '{}'", key.0);
                }
            } else {
                // eprintln!("[DEBUG] parse error for key '{}'", key.0);
            }
        }

        freq.into_iter()
            .max_by(|(a_val, a_cnt), (b_val, b_cnt)| {
                a_cnt.cmp(b_cnt)
                    .then_with(|| a_val.cmp(b_val)) // smaller value wins on tie
            })
            .map(|(nn, cnt)| (nn.into_inner(), cnt))
    }
    
    pub fn mode_from_numkey_vec(&self, v: &[NumKey]) -> f64 {
        let mut freq: HashMap<NotNan<f64>, usize> = HashMap::new();

        for nk in v {
            if let Ok(val) = nk.0.parse::<f64>() {
                if let Ok(nn) = NotNan::new(val) {
                    *freq.entry(nn).or_insert(0) += 1;
                }
            }
        }

        match freq.into_iter().max_by_key(|&(_, count)| count) {
            Some((nn, _count)) => nn.into_inner(), // modal value
            None => f64::NAN,                      // empty or no valid numbers
        }
    }

    /// Weighted variance over f64 values parsed from NumKey, weights = vec.len().
    /// `sample = true`  -> unbiased (n-1) using total count as n (since weights are counts)
    /// `sample = false` -> population variance (/ n)
    fn variance_from_btreemapptr(&self,
        map: &BTreeMap<NumKey, Vec<usize>>,
        sample: bool
    ) -> f64 {
        let mut sum_w  = 0.0_f64;   // total weight (total count)
        let mut mean   = 0.0_f64;   // running mean
        let mut m2     = 0.0_f64;   // sum of weighted squared deviations

        for (k, v) in map {
            let w = v.len() as f64;          // weight = occurrences
            if w == 0.0 { continue; }

            // parse key to f64
            let x = match k.0.parse::<f64>() {
                Ok(val) if val.is_finite() => val,
                _ => {
                    //eprintln!("[variance] skipping non-finite/parse-fail: '{}'", k.0);
                    continue;
                }
            };

            // Weighted Welford update
            let sum_w_new = sum_w + w;
            let delta = x - mean;
            mean += (w / sum_w_new) * delta;
            m2   += w * delta * (x - mean);
            sum_w = sum_w_new;
        }

        if sum_w == 0.0 { return f64::NAN; }

        if sample {
            if sum_w <= 1.0 { return f64::NAN; }
            m2 / (sum_w - 1.0)
        } else {
            m2 / sum_w
        }
    }
    
    pub fn variance_from_btreemap(&self,
        map: &BTreeMap<&NumKey, &Vec<usize>>,
        sample: bool
    ) -> f64 {
        let mut sum_w  = 0.0_f64;   // total weight (total count)
        let mut mean   = 0.0_f64;   // running mean
        let mut m2     = 0.0_f64;   // sum of weighted squared deviations

        for (k, v) in map {
            let w = v.len() as f64;          // weight = occurrences
            if w == 0.0 { continue; }

            // parse key to f64
            let x = match k.0.parse::<f64>() {
                Ok(val) if val.is_finite() => val,
                _ => {
                    //eprintln!("[variance] skipping non-finite/parse-fail: '{}'", k.0);
                    continue;
                }
            };

            // Weighted Welford update
            let sum_w_new = sum_w + w;
            let delta = x - mean;
            mean += (w / sum_w_new) * delta;
            m2   += w * delta * (x - mean);
            sum_w = sum_w_new;
        }

        if sum_w == 0.0 { return f64::NAN; }

        if sample {
            if sum_w <= 1.0 { return f64::NAN; }
            m2 / (sum_w - 1.0)
        } else {
            m2 / sum_w
        }
    }

    pub fn variance_from_numkey_vec(&self, v: &[NumKey], sample: bool) -> f64 {
        let mut n   = 0.0_f64; // count
        let mut mean = 0.0_f64;
        let mut m2   = 0.0_f64;

        for nk in v {
            // assuming NumKey.0 is a String (or similar)
            let x = match nk.0.parse::<f64>() {
                Ok(val) if val.is_finite() => val,
                _ => {
                    //eprintln!("[variance] skipping non-finite/parse-fail: '{}'", nk.0);
                    continue;
                }
            };

            n += 1.0;
            let delta = x - mean;
            mean += delta / n;
            m2   += delta * (x - mean);
        }

        if n == 0.0 {
            return f64::NAN;
        }

        if sample {
            if n <= 1.0 { return f64::NAN; }
            m2 / (n - 1.0)  // sample variance
        } else {
            m2 / n          // population variance
        }
    }

    fn stddevptr(& self,
                 map: &BTreeMap<NumKey, Vec<usize>>,
                 sample: bool) -> f64 {
        self.variance_from_btreemapptr(map, sample).sqrt()
    }  


    pub fn stddev_from_numkey_vec(&self, v: &[NumKey], sample: bool) -> f64 {
        self.variance_from_numkey_vec(v, sample).sqrt()
    }

    /// Weighted median over f64s parsed from NumKey.
    /// Weight = vec.len() for each key. Skips non-numeric / non-finite values.
    /// For even total count, returns the average of the two middle values (expanded-data definition).
    pub fn median_from_btreemapptr(&self, map: &BTreeMap<NumKey, Vec<usize>>) -> f64 {
        // 1) Collect numeric (value, weight)
        let mut vals: Vec<(f64, usize)> = map.iter()
            .filter_map(|(k, v)| {
                let w = v.len();
                if w == 0 { return None; }
                k.0.parse::<f64>().ok()
                    .filter(|x| x.is_finite())
                    .map(|x| (x, w))
            })
            .collect();

        // 2) Total expanded length
        let n: usize = vals.iter().map(|&(_, w)| w).sum();
        if n == 0 { return f64::NAN; }

        // 3) Sort by numeric value (NumKey is string-ordered, so we sort numerically here)
        vals.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        // Helper: value at expanded index without expanding
        let value_at = |target_idx: usize| -> f64 {
            let mut cum = 0usize;
            for (val, w) in vals.iter() {
                let next = cum + *w;
                if target_idx < next {
                    return *val;
                }
                cum = next;
            }
            vals.last().unwrap().0 // safe fallback if target_idx == n-1
        };

        // 4) Pick middle(s) like an expanded vector would
        if n % 2 == 1 {
            // odd: middle index n/2
            value_at(n / 2)
        } else {
            // even: average of the two middles (n/2 - 1, n/2)
            let lo = value_at(n / 2 - 1);
            let hi = value_at(n / 2);
            (lo + hi) / 2.0
        }
    }
    
    fn median_from_numkey_vec(&self, v: &[NumKey]) -> f64 {
        // 1) Collect numeric values (skip parse failures / non-finite)
        let mut vals: Vec<f64> = v.iter()
            .filter_map(|nk| nk.0.parse::<f64>().ok())
            .filter(|x| x.is_finite())
            .collect();

        // 2) Handle empty case
        let n = vals.len();
        if n == 0 {
            return f64::NAN;
        }

        // 3) Sort numerically
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // 4) Median
        if n % 2 == 1 {
            vals[n / 2] // odd: middle element
        } else {
            let lo = vals[n / 2 - 1];
            let hi = vals[n / 2];
            (lo + hi) / 2.0
        }
    }

    /// Percentile (p in [0,100]) using nearest-rank (inclusive).
    /// Works directly on BTreeMap<NumKey, Vec<usize>>, where `Vec` length is the weight.
    fn percentile_nearest_rank_from_btreeptr(
        &self,
        map: &BTreeMap<NumKey, Vec<usize>>,
        p: f64,
    ) -> f64 {
        // Collect (value, weight)
        let mut values: Vec<(f64, usize)> = map.iter()
            .filter_map(|(k, v)| {
                match k.0.parse::<f64>() {
                    Ok(val) if val.is_finite() => Some((val, v.len())),
                    _ => None, // skip bad/non-numeric
                }
            })
            .collect();

        // Total count (weights)
        let total: usize = values.iter().map(|(_, w)| *w).sum();
        if total == 0 {
            return f64::NAN;
        }

        let p = p.clamp(0.0, 100.0);
        let r = (p / 100.0 * total as f64).ceil().clamp(1.0, total as f64) as usize;

        // Iterate sorted values (NumKey is already in order in BTreeMap)
        let mut cumulative = 0usize;
        for (val, w) in values {
            cumulative += w;
            if cumulative >= r {
                return val;
            }
        }

        f64::NAN // fallback (shouldn't reach)
    }
    
    pub fn percentile_nearest_rank_from_vec(&self, v: &[NumKey], p: f64) -> f64 {
        // 1) Collect numeric values
        let mut values: Vec<f64> = v.iter()
            .filter_map(|nk| nk.0.parse::<f64>().ok())
            .filter(|x| x.is_finite())
            .collect();

        // 2) Handle empty input
        let total = values.len();
        if total == 0 {
            return f64::NAN;
        }

        // 3) Sort numerically
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // 4) Nearest-rank index (1-based rule)
        let p = p.clamp(0.0, 100.0);
        let r = (p / 100.0 * total as f64).ceil().clamp(1.0, total as f64) as usize;

        // 5) Return r-th smallest (convert to 0-based index)
        values[r - 1]
    }

    /// Weighted linear-interpolated percentile (Excel PERCENTILE.INC) over f64 values parsed from NumKey.
    /// Weights = vec.len() for each key. Skips entries that fail to parse or aren't finite.
    fn percentile_linear_from_btreeptr(
        &self,
        map: &BTreeMap<NumKey, Vec<usize>>,
        p: f64,
    ) -> f64 {
        // Collect numeric values with their weights
        let mut vals: Vec<(f64, usize)> = map.iter()
            .filter_map(|(k, v)| {
                let w = v.len();
                if w == 0 { return None; }
                k.0.parse::<f64>().ok().filter(|x| x.is_finite()).map(|x| (x, w))
            })
            .collect();

        // Total number of observations (expanded length)
        let n: usize = vals.iter().map(|&(_, w)| w).sum();
        if n == 0 {
            return f64::NAN;
        }

        // Sort by numeric value (NumKey order is lexical; we want numeric)
        vals.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let p = p.clamp(0.0, 100.0);
        if p == 0.0 {
            return vals.first().map(|(v, _)| *v).unwrap();
        }
        if p == 100.0 {
            return vals.last().map(|(v, _)| *v).unwrap();
        }

        // rank in [0, n-1]
        let rank = (p / 100.0) * ((n - 1) as f64);
        let lo_idx = rank.floor() as usize;
        let hi_idx = rank.ceil() as usize;

        // Helper to fetch the value at an expanded index without expanding
        let mut value_at = |target_idx: usize| -> f64 {
            let mut cum = 0usize;
            for (val, w) in vals.iter() {
                let next = cum + *w;
                if target_idx < next {
                    return *val;
                }
                cum = next;
            }
            // Fallback; should not happen if indices are within [0, n-1]
            vals.last().unwrap().0
        };

        if lo_idx == hi_idx {
            value_at(lo_idx)
        } else {
            let lo_v = value_at(lo_idx);
            let hi_v = value_at(hi_idx);
            let w = rank - lo_idx as f64; // interpolation weight in [0,1]
            (1.0 - w) * lo_v + w * hi_v
        }
    }


    pub fn percentile_linear_from_btree(
        &self,
        map: &BTreeMap<&NumKey, &Vec<usize>>,
        p: f64,
    ) -> f64 {
        // Collect numeric values with their weights
        let mut vals: Vec<(f64, usize)> = map.iter()
            .filter_map(|(k, v)| {
                let w = v.len();
                if w == 0 { return None; }
                k.0.parse::<f64>().ok().filter(|x| x.is_finite()).map(|x| (x, w))
            })
            .collect();

        // Total number of observations (expanded length)
        let n: usize = vals.iter().map(|&(_, w)| w).sum();
        if n == 0 {
            return f64::NAN;
        }

        // Sort by numeric value (NumKey order is lexical; we want numeric)
        vals.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let p = p.clamp(0.0, 100.0);
        if p == 0.0 {
            return vals.first().map(|(v, _)| *v).unwrap();
        }
        if p == 100.0 {
            return vals.last().map(|(v, _)| *v).unwrap();
        }

        // rank in [0, n-1]
        let rank = (p / 100.0) * ((n - 1) as f64);
        let lo_idx = rank.floor() as usize;
        let hi_idx = rank.ceil() as usize;

        // Helper to fetch the value at an expanded index without expanding
        let mut value_at = |target_idx: usize| -> f64 {
            let mut cum = 0usize;
            for (val, w) in vals.iter() {
                let next = cum + *w;
                if target_idx < next {
                    return *val;
                }
                cum = next;
            }
            // Fallback; should not happen if indices are within [0, n-1]
            vals.last().unwrap().0
        };

        if lo_idx == hi_idx {
            value_at(lo_idx)
        } else {
            let lo_v = value_at(lo_idx);
            let hi_v = value_at(hi_idx);
            let w = rank - lo_idx as f64; // interpolation weight in [0,1]
            (1.0 - w) * lo_v + w * hi_v
        }
    }

    fn percentile_linear_from_vec(&self, v: &[NumKey], p: f64) -> f64 {
        // 1) Collect numeric values, skipping bad/non-finite entries
        let mut vals: Vec<f64> = v.iter()
            .filter_map(|nk| nk.0.parse::<f64>().ok())
            .filter(|x| x.is_finite())
            .collect();

        // 2) Handle empty
        let n = vals.len();
        if n == 0 {
            return f64::NAN;
        }

        // 3) Sort numerically
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // 4) Clamp p to [0, 100] and handle endpoints
        let p = p.clamp(0.0, 100.0);
        if p == 0.0 {
            return vals[0];
        }
        if p == 100.0 {
            return vals[n - 1];
        }

        // 5) Rank in [0, n-1]
        let rank = (p / 100.0) * ((n - 1) as f64);
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;

        // 6) Interpolate
        if lo == hi {
            vals[lo]
        } else {
            let w = rank - lo as f64; // in [0,1]
            (1.0 - w) * vals[lo] + w * vals[hi]
        }
    }



    fn intersection_unique_ordered(&self, a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_empty() || b.is_empty() {
            return Vec::new();
        }

        // Build set from b for fast membership checks
        let mem: HashSet<usize> = b.iter().copied().collect();

        let mut seen = HashSet::new();   // to avoid duplicates in result
        let mut res = Vec::new();

        for &x in a {
            if mem.contains(&x) && seen.insert(x) {
                res.push(x); // preserves order of `a`
            }
        }

        res
    }

    pub fn intersection_unique_unordered(&self, a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_empty() || b.is_empty() {
            return Vec::new();
        }

        // Build HashSet from the smaller slice; scan the larger one.
        let (small, large) = if a.len() <= b.len() { (a, b) } else { (b, a) };

        let mut mem = HashSet::with_capacity(small.len());
        mem.extend(small.iter().copied());

        // Deduplicate results on the fly.
        let mut res = HashSet::with_capacity(small.len());
        for &x in large {
            if mem.contains(&x) {
                res.insert(x);
            }
        }
        res.into_iter().collect() // unordered; sort if you need determinism
    }

pub fn append_line_and_track(&mut self, file_name: &str, new_line: &str, timestamp: &str, key_str: &String) -> std::io::Result<()> {
    use std::path::PathBuf;

    let file_path = PathBuf::from(&self.datadir).join(format!("{}.jblox", file_name));

    // ----- Build the record (unchanged) -----
    let placeholder_size_str = "0".repeat(self.size_digits);
    let placeholder_key_size_str = "0".repeat(self.size_digits_keys);

    let recdilimter = self.settings.recorddelimiter;
    let indexnamevaluedelimiter = self.settings.indexnamevaluedelimiter;
    let mut updated_line = format!(
        "00-{timestamp}`{placeholder_size_str}{indexnamevaluedelimiter}{placeholder_key_size_str}{recdilimter}{new_line}\n"
    );

    // Encode updated_line to UCS-2 to get actual byte length
    let ucs2_data: Vec<u16> = updated_line.chars()
        .map(|c| if c as u32 <= 0xFFFF { c as u16 } else { '?' as u16 })
        .collect();
    let line_bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(ucs2_data.as_ptr() as *const u8, ucs2_data.len() * 2)
    };
    let total_size = line_bytes.len();

    // key_str length in UCS-2 (validate BMP)
    let keystrlen = if key_str.chars().any(|ch| (ch as u32) > 0xFFFF) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "String contains non-BMP characters; not representable in UCS-2",
        ));
    } else {
        key_str.chars().count() * 2
    };

    // Now update size fields with actual values
    let size_str = format!("{:0width$}", total_size, width = self.size_digits);
    let size_key_str = format!("{:0width$}", keystrlen, width = self.size_digits_keys);
    updated_line = format!(
        "00-{timestamp}`{size_str}{indexnamevaluedelimiter}{size_key_str}{recdilimter}{new_line}\n"
    );

    // Re-encode with correct size
    let ucs2_data: Vec<u16> = updated_line.chars()
        .map(|c| if c as u32 <= 0xFFFF { c as u16 } else { '?' as u16 })
        .collect();
    let line_bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(ucs2_data.as_ptr() as *const u8, ucs2_data.len() * 2)
    };

    let record_delim = self.settings.recorddelimiter.to_string();
    let index_delim = self.settings.indexdelimiter.to_string();
    let indexnamevalue_delim = self.settings.indexnamevaluedelimiter.to_string();

    // ===== Ensure per-file mmap exists (per-file locking path) =====
    // Try to get Arc<Mutex<MmapMut>> quickly under a read lock
    let mut mm_arc_opt = {
        let state = self.state.lock().unwrap();
        let r = state.file_mmap_map.read().unwrap();
        r.get(file_name).cloned()
    };

    if mm_arc_opt.is_none() {
        // Not present yet: initialize by calling load_existing_file (will insert per-file entries)
        // Clone shared handles to pass into loader
        let (mmap_w, mmap_r) = {
            let w = {
                let s = self.state.lock().unwrap();
                std::sync::Arc::clone(&s.file_mmap_map)
            };
            let r = {
                let sr = self.stateR.lock().unwrap();
                std::sync::Arc::clone(&sr.file_mmap_map_forread)
            };
            (w, r)
        };

        // The loader ensures the file exists, sets size, and inserts arcs into both maps.
        // We pass maxrecords=0, inputoffset=0 to do a single pass (or rely on loader's behavior).
        load_existing_file(
            &self.settings.initfilesize,
            self.settings.newfilesizemultiplier,
            &file_path.to_string_lossy(),
            &record_delim,
            &index_delim,
            &indexnamevalue_delim,
            self.settings.enableviewdelete,
            self.settings.low_ram_mode,
            self.settings.MADVISE_CHUNK,
            self.size_digits,
            self.size_digits_keys,
            self.settings.maxrecordlength,
            0, // maxrecords (loader doesn't need to page in append case)
            0, // inputoffset
            &mut self.file_size_map,
            &mmap_w,
            &mmap_r,
            &mut self.file_line_map,
            &mut self.file_key_line_map,
            &mut self.file_key_pointer_map,
            &mut self.file_key_pointer_fordeleted_map,
            &self.settings,
        )?;

        // Re-fetch after initialization
        mm_arc_opt = {
            let state = self.state.lock().unwrap();
            let r = state.file_mmap_map.read().unwrap();
            r.get(file_name).cloned()
        };
    }

    let mm_arc = mm_arc_opt.ok_or_else(|| {
        // Make sure this refers to the IO *type*, not the trait
        std::io::Error::new(std::io::ErrorKind::NotFound,
                            "mmap_mut entry missing after initialization")
    })?;

    // ===== Append under per-file lock only =====
    let mut mm = mm_arc.lock().unwrap();

    // Determine start offset from file_size_map (default 0 if missing)

    let mut start = 0;
    if let Some(size) = self.file_size_map.get(file_name).copied() {
        start = size;
    }

    self.file_size_map.entry(file_name.to_string()).or_insert(0).clone();
    let end = start + line_bytes.len();

    if end > mm.len() {
        // If you want auto-grow, implement: grow file, remap, and replace the Arc<Mutex<MmapMut>> entry.
        // For now, keep your previous behavior (error out).
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Write exceeds mmap size",
        ));
    }

    mm[start..end].copy_from_slice(line_bytes);
    
    if(self.settings.strongdurablemode){
        if let Err(e) = mm.flush() {
            // best-effort log; ignore logging failure to preserve current flow
            let _ = self.log_error_message(&format!(
                "{}-{}-FLUSH-{}-ERR kind={:?} os={:?} msg={}",
                timestamp,
                file_name,
                new_line.clone(),
                e.kind(),
                e.raw_os_error(),
                e
            ));
        }
        else{
            self.log_message(&format!("{}-0-{}-I", timestamp,start))?;
        }   
    }
    else{
        self.log_message(&format!("{}-0-{}-I", timestamp,start))?;

    }


    // Update in-memory indexes
    self.file_size_map.insert(file_name.to_string(),end.clone());
    self.file_line_map.get_mut(file_name).unwrap().insert(start, line_bytes.len());
    self.file_key_line_map.get_mut(file_name).unwrap().insert(start, keystrlen);

    if let Some(key_map) = self.file_key_pointer_map.get_mut(file_name) {
        for part in key_str.trim().split(index_delim.as_str()) {
            if let Some(dash_pos) = part.find(indexnamevalue_delim.as_str()) {
                let key_name = &part[..dash_pos];
                let key_value = &part[dash_pos + 1..];
                key_map
                    .entry(key_name.to_string())
                    .or_default()
                    .entry(NumKey(key_value.into()))
                    .or_default()
                    .push(start);
            }
        }
    }

    // Capacity warnings
    let ratio = end as f64 / mm.len() as f64;
    let decimal_part = (ratio.fract() * 100.0).round() / 100.0;
    if decimal_part > 0.95 {
        eprintln!("⚠️  ERROR: data file size {}%, stopping jbloxdb.", decimal_part * 100.0);
        process::exit(0);
    } else if decimal_part > 0.85 {
        eprintln!("⚠️  Warning data file size {}%, increase init file size in config file and restart jbloxdb to increase allowed size.", decimal_part * 100.0);
        eprintln!("⚠️  Warning data file size {}%, Process will stop automatically when file size reaches 98%.", decimal_part * 100.0);
    }

    Ok(())
}


pub fn print_line_forpointer_uselen(
    &self,
    file_name: &str,
    start_ptr_addr: usize,
    includedelete: bool,
) -> std::io::Result<String> {
    use memmap2::Mmap;

    let mut retstr = String::new();
    let _max_len: usize = self.settings.maxrecordlength;

    // record length for this pointer
    let lines = self.file_line_map.get(file_name).ok_or_else(|| {
        io::Error::new(ErrorKind::NotFound, format!("missing file entry for {}", file_name))
    })?;

    let mut reclength = *lines.get(&start_ptr_addr).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("missing pointer entry at {}", start_ptr_addr),
        )
    })?;

   
    // Get Arc<Mmap> for this file under a short map read-lock, then drop all locks
    let mmap_arc: std::sync::Arc<Mmap> = {
        let stateR = self.stateR.lock().unwrap();
        let r = stateR.file_mmap_map_forread.read().unwrap();
        r.get(file_name)
            .cloned()
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "read mmap missing for file"))?
    }; // drop stateR & map locks here

    let start = start_ptr_addr;
    let end = start + reclength;

    // bounds check
    if end > mmap_arc.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Requested range exceeds mmap size",
        ));
    }

    let bytes = &mmap_arc[start..end];

    // 3) Turn every pair into a u16 code unit
    let code_units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .collect();

    // 4) Decode (lossy to avoid panics on malformed input)
    let line2 = String::from_utf16_lossy(&code_units);

    // Respect delete visibility
    if includedelete {
        retstr = line2;
    } else if line2.starts_with("00") {
        retstr = line2;
    } else {
        return Ok(String::new());
    }

    Ok(retstr)
}


////////////////////////////////////////////////////////////////// 


pub fn insert_duplicate_frmObject(&mut self, json: &Value,timestamp: &str) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};

    // --- Normalize JSON at the top (avoid clone unless needed) ---
    let needs_pk_fix = match json.get("primkey").and_then(Value::as_str) {
        Some(pk) => !pk.split(',').any(|t| t.trim() == "recid"),
        None => return Err(Error::new(ErrorKind::InvalidData, "Missing 'primkey' field")),
    };
    let needs_recid_fix = match json.get("recid") {
        Some(Value::String(s)) => s != timestamp,
        Some(_) => true, // non-string -> fix
        None => true,
    };

    let json_cow: Cow<'_, Value> = if !needs_pk_fix && !needs_recid_fix {
        Cow::Borrowed(json)
    } else {
        let mut owned = json.clone();
        let obj = owned.as_object_mut().ok_or_else(|| {
            Error::new(ErrorKind::InvalidData, "JSON input is not an object")
        })?;

        // Upsert/overwrite root "recid"
        obj.insert("recid".to_string(), Value::String(timestamp.to_string()));

        // Ensure "primkey" includes token "recid"
        let pk = obj.get_mut("primkey").ok_or_else(|| {
            Error::new(ErrorKind::InvalidData, "Missing 'primkey' field")
        })?;
        let pk_str = pk.as_str().ok_or_else(|| {
            Error::new(ErrorKind::InvalidData, "'primkey' must be a string")
        })?;
        if !pk_str.split(',').any(|t| t.trim() == "recid") {
            let mut new_pk = pk_str.to_owned();
            if !new_pk.trim().is_empty() {
                new_pk.push(',');
            }
            new_pk.push_str("recid");
            *pk = Value::String(new_pk);
        }

        Cow::Owned(owned)
    };

    // Use the (possibly) normalized JSON from here on
    let json = json_cow;

    let file_name = json.get("keyobj")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;

    //before going further, check if record is duplicate based on the primary key
    //remember, though from users perspective, they can add duplicate record, internally
    //each record is unique as primary key also contains internally generated 'recid'
    //so if record, with primary key recid included, already exist: do not insert record
    let primkey_map: IndexMap<String, String> = self.extract_prim_keyname_value_map(&json)?;
    let (duplicatecheckptrs, lines)  
        = self.get_fromkey(file_name, 
                            &primkey_map, 
                            "0",
                            "0", 
                            true,
                            false,
                            false,
                            0,
                            1);    
    
    if(duplicatecheckptrs.len() > 0){
        //record already exist
        //log it 
        self.log_message(&format!("{}-0-{}-I-Duplicate", timestamp, duplicatecheckptrs[0]))?;

    }
    else{
        if let Some(obj) = json.as_object() {
        if let key_str = {
            let pairs: Vec<(&str, &Value)> = self.extract_key_value_multiple_forInsert(&json);
            let parts: Vec<String> = pairs
                .into_iter()
                .map(|(key, value)| {
                    let val_str = match value {
                        Value::String(s) => s.clone(),
                        _ => value.to_string().trim_matches('"').to_string(),
                    };
                    //replace indexnamevaluedelimiter with repindexnamevaluedelimiter
                    let mut key_t = key.to_string();
                    let mut val_str_t = val_str.clone();
                    if(key_t.contains(self.settings.indexnamevaluedelimiter)){
                        key_t = key_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                    }
                    if(key_t.contains(self.settings.indexdelimiter)){
                        key_t = key_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                    }
                    if(key_t.contains(self.settings.recorddelimiter)){
                        key_t = key_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                    }
                    if(val_str_t.contains(self.settings.indexnamevaluedelimiter)){
                        val_str_t = val_str_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                    }
                    if(val_str_t.contains(self.settings.indexdelimiter)){
                        val_str_t = val_str_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                    }
                    if(val_str_t.contains(self.settings.recorddelimiter)){
                        val_str_t = val_str_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                    }                          
                    format!("{}{}{}", key_t,self.settings.indexnamevaluedelimiter.to_string(), val_str_t)
                })
                .collect();
                parts.join(&self.settings.indexdelimiter.to_string())}{
        

                // Step 4: Minify JSON and prepare line
                let new_line = format!("{}{}{}", key_str,self.settings.recorddelimiter, json);
                // Step 5: Append to file
                self.append_line_and_track(file_name, &new_line,timestamp,&key_str)?;
            } else {
                self.log_message(&format!("{}-0-()-I-{}", timestamp,"Invalid or missing 'key/primkey'"))?;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid or missing 'key/primkey' in JSON",
                ));
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "JSON input is not an object",
            ));
        }
    }
    Ok(())
}


    pub fn insertduplicate(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {

        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-11-insertduplicate-{}",currtstmp, compact_jsonstr))?;

        // Do not check for duplicates
        let result =  self.insert_main(json_str, false, &currtstmp);
        match &result {
           Ok(vec) => {}
                //println!("{}-{:?}", currtstmp, vec);
           //}
            Err(e) => {
                self.log_message(&format!("{}-{}",currtstmp, e));
            }
        }         
        self.log_message(&format!("{}-00-Done", currtstmp));     

        result            
    }

    pub fn insert(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {
       
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-11-insert-{}",currtstmp, compact_jsonstr))?;


        let result =  self.insert_main(json_str, true, &currtstmp);
        match &result {
            Ok(vec) => {}
                //println!("{}-{:?}", currtstmp, vec);
            //}
            Err(e) => {
                self.log_message(&format!("{}-{}",currtstmp, e));
            }
        }         
        self.log_message(&format!("{}-00-Done", currtstmp));     

        result        
    }

    pub fn insert_main(&mut self, json_str: &str, check_duplicates: bool, timestamp: &str) -> std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};

        self.log_message(&format!("{}-1-()-T-I", timestamp))?;
        

        // Step 1: Parse the input JSON string into a Value.
        let mut json: Value = serde_json::from_str(&json_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // --- NEW: normalize recid + primkey at the JSON layer ---
        let obj = json.as_object_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "JSON input is not an object")
        })?;

        // 1) Upsert root "recid" with the given timestamp (overwrite if present)
        obj.insert("recid".to_string(), Value::String(timestamp.to_string()));

        // 2) Ensure top-level "primkey" (string) includes token "recid"
        let pk = obj.get_mut("primkey").ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Missing 'primkey' field")
        })?;

        let pk_str = pk.as_str().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "'primkey' must be a string")
        })?;

        // Fast check (avoid alloc if already present as a token)
        let has_recid = pk_str.split(',').any(|t| t.trim() == "recid");

        if !has_recid {
            // Make it owned/mutable once (only when we actually need to modify)
            let mut new_pk = pk_str.to_owned();
            if !new_pk.trim().is_empty() {
                new_pk.push(',');
            }
            new_pk.push_str("recid");
            *pk = Value::String(new_pk);
        }

        // --- END NEW ---

        let file_name = json.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;

        let mut recordwithsamepureprimarykey = true;
        let mut readytoinsertduplicate = false;
        let mut readytoinsertnonduplicate = false;

        let mut duplicatepointerfornonduplicate : usize = 0;
        let mut duplicatepointerforduplicate : usize = 0;
        
        if let Ok(ptrs) 
            = self.getmainforduplicatecheck(&json,
                            true,
                            false,
                        true,) {
            if ptrs.is_empty() {
                recordwithsamepureprimarykey = false;
                readytoinsertnonduplicate = true;
                readytoinsertduplicate = true;
            }
            else{
                duplicatepointerfornonduplicate = ptrs[0];
                duplicatepointerforduplicate = ptrs[0];
                readytoinsertnonduplicate = false;
            }
        }
        if (!check_duplicates && recordwithsamepureprimarykey) {
            //check again, this time with recid
            if let Ok(ptrs) 
                = self.getmainforduplicatecheck(&json,
                               true,
                               false,
                            false) {
                if ptrs.is_empty() {
                    readytoinsertduplicate = true;
                }
                else{
                    readytoinsertduplicate = false;
                    duplicatepointerforduplicate = ptrs[0];
                }                
            }
        }

        //check duplicates
        // Call the existing method
        if check_duplicates {
            if !readytoinsertnonduplicate{
                    self.log_message(&format!("{}-0-{}-I-Duplicate", timestamp,duplicatepointerfornonduplicate))?;                            
                    return Err(Error::new(ErrorKind::Other, "Duplicate Record."));
            }
        }
        else{
            if !readytoinsertduplicate{
                    self.log_message(&format!("{}-0-{}-I-Duplicate", timestamp,duplicatepointerforduplicate))?;                            
                    return Err(Error::new(ErrorKind::Other, "Duplicate Record."));
            }            
        }

        // Step 2: Extract key value from JSON
        let key_str = {
            let pairs: Vec<(&str, &Value)> = self.extract_key_value_multiple_forInsert(&json);
            let parts: Vec<String> = pairs
                .into_iter()
                .map(|(key, value)| {
                    let val_str = match value {
                        Value::String(s) => s.clone(),
                        _ => value.to_string().trim_matches('"').to_string(),
                    };
                    //replace indexnamevaluedelimiter with repindexnamevaluedelimiter
                    let mut key_t = key.to_string();
                    let mut val_str_t = val_str.clone();
                    if(key_t.contains(self.settings.indexnamevaluedelimiter)){
                        key_t = key_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                    }
                    if(key_t.contains(self.settings.indexdelimiter)){
                        key_t = key_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                    }
                    if(key_t.contains(self.settings.recorddelimiter)){
                        key_t = key_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                    }
                    if(val_str_t.contains(self.settings.indexnamevaluedelimiter)){
                        val_str_t = val_str_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                    }
                    if(val_str_t.contains(self.settings.indexdelimiter)){
                        val_str_t = val_str_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                    }
                    if(val_str_t.contains(self.settings.recorddelimiter)){
                        val_str_t = val_str_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                    }                          
                    format!("{}{}{}", key_t,self.settings.indexnamevaluedelimiter.to_string(), val_str_t)
                })
                .collect();
            parts.join(&self.settings.indexdelimiter.to_string())
        };

        // Step 4: Minify JSON and prepare line
        let new_line = format!("{}{}{}", key_str,self.settings.recorddelimiter, json);
        
        // Step 5: Append to file
        self.append_line_and_track(file_name, &new_line,timestamp,&key_str)?;

                    
        Ok(vec!["done".to_string()])
    }


fn extract_specific_key_value<'a>(&self, json: &'a Value, keyname: &str) -> Option<&'a Value> {
    if let Some(obj) = json.as_object() {
        if let Some(value) = obj.get(keyname) {
            return Some(value);
        } else {
            return self.find_key_recursively(json, keyname);
        }
    }
    None
}

fn extract_keyobj_value<'a>(&self, json: &'a Value) -> Option<(&'a str, &'a Value)> {
    if let Some(obj) = json.as_object() {
        if let Some(value) = obj.get("keyobj") {
            return Some(("keyobj", value));
        }
    }
    None
}

    fn extract_key_value_multiple_forInsert<'a>(&self, json: &'a Value) -> Vec<(&'a str, &'a Value)> {
        let mut results = Vec::new();

        if let Some(obj) = json.as_object() {
            if let Some(Value::String(key_names)) = obj.get("key") {
                for key_name_str in key_names.split(',').map(|s| s.trim()) {
                    if let Some(value) = obj.get(key_name_str) {
                        results.push((key_name_str, value));
                    } else if let Some(found_value) = self.find_key_recursively(json, key_name_str) {
                        results.push((key_name_str, found_value));
                    }
                }
            }
            let mut seen: FxHashSet<&str> = results.iter().map(|(k, _)| *k).collect();
            //also key primkey
            if let Some(Value::String(key_names)) = obj.get("primkey") {
                for key_name_str in key_names.split(',').map(|s| s.trim()) {
                    if !seen.insert(key_name_str) { continue; } // already present

                    if let Some(value) = obj.get(key_name_str) {
                        results.push((key_name_str, value));
                    } else if let Some(found_value) = self.find_key_recursively(json, key_name_str) {
                        results.push((key_name_str, found_value));
                    }
                }
            }


        }

        results
    }


    fn find_key_recursively<'a>(&self, json: &'a Value, key: &str) -> Option<&'a Value> {
        match json {
            Value::Object(map) => {
                if let Some(val) = map.get(key) {
                    return Some(val);
                }
                for (_k, v) in map {
                    if let Some(found) = self.find_key_recursively(v, key) {
                        return Some(found);
                    }
                }
            }
            Value::Array(arr) => {
                for v in arr {
                    if let Some(found) = self.find_key_recursively(v, key) {
                        return Some(found);
                    }
                }
            }
            _ => {}
        }
        None
    }

    pub fn delete(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {
        let currtstmp: String = self.current_timestamp();
        let result =  self.deleteWTS(json_str, currtstmp.clone()); // delete last
        result
    }

    pub fn deleteWTS(&mut self, json_str: &str,currtstmp: String) ->std::io::Result<Vec<String>> {
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-11-delete-{}", currtstmp, compact_jsonstr))?;

        let result =  self.deletemain(json_str, false,&currtstmp); // delete last
        self.log_message(&format!("{}-00-Done", currtstmp));     

        result
    }

    pub fn deleteall(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {
        let currtstmp: String = self.current_timestamp();
        let result =  self.deleteallWTS(json_str, currtstmp.clone()); // delete last
        result
    }

    pub fn deleteallWTS(&mut self, json_str: &str,currtstmp: String) ->std::io::Result<Vec<String>> {

        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-11-deleteall-{}", currtstmp, compact_jsonstr))?;
 
        let result = self.deletemain(json_str, true,&currtstmp); // delete all
        self.log_message(&format!("{}-00-Done", currtstmp));     
        
        result        
    }

    pub fn deletemain(&mut self, json_str: &str, delete_all: bool,timestamp: &String) ->std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};
        // Step 1: Parse the input JSON string
        let json: Value = serde_json::from_str(json_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;


        // Step 3: Validate JSON is an object
        if !json.is_object() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "JSON input is not an object",
            ));
        }
        let file_name = json.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;

        let mut key_map: IndexMap<String, String> = IndexMap::new();
        //Step 4 A: Extract primary_key-value map from json
        //primary key is optional in delete

        if json.get("primkey").is_some(){
            let primkey_map: IndexMap<String, String> = self.extract_prim_keyname_value_map(&json)?;
            for (k, v) in primkey_map {
                key_map.entry(k).or_insert(v);
            }            
        }
        if(key_map.len() == 0){
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing Primary Key; 'primkey' is required for delete",
            ));            
        }
        let mut ptrs: Vec<usize> = Vec::new();
        if(delete_all){
            let (ptrs_match, lines)  
                = self.get_fromkey(file_name, 
                                    &key_map, 
                                    "0",
                                    "0", 
                                    false,
                                    false,
                                    false,
                                    0,
                                    1);     

            ptrs = ptrs_match;
        }
        else{
            let (ptrs_match, lines)  
                = self.get_fromkey(file_name, 
                                    &key_map, 
                                    "0",
                                    "0", 
                                    true,
                                    false,
                                    false,
                                    0,
                                    1);  
            ptrs = ptrs_match;

        }
       
        let ptrcount = ptrs.len();
        if(ptrcount > 0){
            for ptr in &ptrs {
                self.log_message(&format!("{}-1-{}-T-D-{}", timestamp,ptr,ptrcount))?;
            }
        
            self.delete_using_pointerVector(file_name, ptrs.clone(),"d".to_string(),timestamp);
        }
        else{
                self.log_message(&format!("{}-0-0-D-{}", timestamp,"Record_Not_found"))?;
        }


        let lines = vec![
            String::from("done")
        ];
        Ok(lines)
    }

pub fn delete_using_pointerVector(&mut self, 
                                 file_name: &str, 
                                 ptrs: Vec<usize>,
                                 optype: String,
                                 timestamp: &str,) -> Result<(), Box<dyn Error>>{ //u: update;d: delete

    // 1) Get Arc<Mutex<MmapMut>> for this file under a short map read-lock
    let mm_arc = {
        let state = self.state.lock().unwrap();
        let r = state.file_mmap_map.read().unwrap();
        r.get(file_name)
            .cloned()
            .ok_or("mmap_mut entry missing for file")?
    }; // drop state & map locks

    // 2) Lock only this file's mmap and mark delete
    {
        let mut mm: MutexGuard<'_, MmapMut> = mm_arc.lock().unwrap();

        let mut indicatorchar = b'2';
        if(optype == "u"){
            indicatorchar     = b'1';
        }

        for ptr in &ptrs {
            mm[*ptr]     = indicatorchar;
            mm[*ptr + 1] = 0x00;
            if let Err(e) = mm.flush() {
                // best-effort log; ignore logging failure to preserve current flow
                let _ = self.log_error_message(&format!(
                    "{}-{}-FLUSH-{}-ERR kind={:?} os={:?} msg={}",
                    timestamp,
                    file_name,
                    *ptr,
                    e.kind(),
                    e.raw_os_error(),
                    e
                ));
            }
            else{
                self.log_message(&format!("{}-0-{}-D", timestamp,*ptr));
            }

        }


    }

    // 3) Update in-memory key length to mark as deleted
    let key_lines = self.file_key_line_map.get_mut(file_name).unwrap();
    for ptr in &ptrs {
        if let Some(old) = key_lines.get(ptr).cloned() {
            if(old < ADD_TOMARK_REC_DELETE_IN_RECORDKEY){
                key_lines.insert(ptr.clone(), old + ADD_TOMARK_REC_DELETE_IN_RECORDKEY);
            }
        } 
    }    

    //mark record as delete, and add to delete tree and delete from main tree 
    //BTree for non-deleted
    let ptrmap_opt = self.file_key_pointer_map.get_mut(file_name); 

    //BTree for deleted
    let mut delete_ptrmap_opt = self.file_key_pointer_fordeleted_map.get_mut(file_name);

    if let Some(ptrmap) = ptrmap_opt{
        let key_lines: &BTreeMap<usize, usize> = 
            self.file_key_line_map
                .get(file_name)
                .ok_or( "file_name not found in file_key_line_map")?;

        // fetch Arc<Mmap> once, under a short lock, then drop locks
        let mmap_arc = {
            let stateR = self.stateR.lock().unwrap();
            let r = stateR.file_mmap_map_forread.read().unwrap(); // Arc<RwLock<HashMap<..>>>
            r.get(file_name)
                .cloned()
                .ok_or("read mmap missing for file")?
        };
        let keystartbuffer = ((3 + TIMESTAMP_LEN + 1) * 2)
                + ((RECORD_SIZE_DIGITCOUNT) * 2) + (1 * 2)
                + (RECORDKEY_SIZE_DIGITCOUNT * 2) + (1 * 2); 

        for ptr in ptrs {

            let keystr_start =
                ptr + keystartbuffer;

            // recorded key length for this record
            let mut key_record_len = *key_lines
                .get(&ptr)
                .ok_or("ptr not found")?;

            // correct the size if record marked as deleted
            if key_record_len >= ADD_TOMARK_REC_DELETE_IN_RECORDKEY {
                key_record_len = key_record_len - ADD_TOMARK_REC_DELETE_IN_RECORDKEY;
            }

            let keystr_end = keystr_start + key_record_len;
            if keystr_end > mmap_arc.len() {
                // out-of-bounds safeguard; skip or handle as you prefer
                continue;
            }

            let keystr_slice = &mmap_arc[keystr_start..keystr_end];

            let keystr_utf16: Vec<u16> = keystr_slice
                .chunks_exact(2)
                .map(|b| u16::from_le_bytes([b[0], b[1]]))
                .collect();

            if let Ok(key_field) = String::from_utf16(&keystr_utf16) {
                for part in key_field.split(self.settings.indexdelimiter) {
                    if let Some(dash_pos) = part.find(self.settings.indexnamevaluedelimiter) {
                        let key_name = &part[..dash_pos];
                        let key_value = &part[dash_pos + 1..];

                        //add corresponding new record in file_key_pointer_fordeleted_map
                        if let Some(mainrectree) = ptrmap.get_mut(key_name){
                            let nk = NumKey::from(key_value.to_string());
                            if let Some(mainrecvec) = mainrectree.get_mut(&nk){
                                //delete ptr from ptrmap
                                if let Some(pos) = mainrecvec.iter().position(|&x| x == ptr) {
                                    mainrecvec.swap_remove(pos); // fast, but reorders
                                }                                

                                //add to delete_ptrmap
                                if let Some(delete_ptrmap) = & mut delete_ptrmap_opt {
                                    delete_ptrmap
                                        .entry(key_name.to_string())
                                        .or_insert_with(BTreeMap::new)   // &mut BTreeMap<NumKey, Vec<usize>>
                                        .entry(key_value.to_string().into())                       // Entry<NumKey, Vec<usize>>
                                        .or_insert_with(Vec::new)        // &mut Vec<usize>
                                        .push(ptr.clone()); 
                                }                                        
                            }   
                        }

                    }
                }
            }
        }

    }
    Ok(())
}

    //restores previous record. How far back depends on the undocount
    pub fn undo(&mut self, json_str: &str) ->std::io::Result<Vec<String>>{
        let currtstmp:String = self.current_timestamp();
        return self.undoWTS(json_str, currtstmp); // delete last
    }

    pub fn undoWTS(&mut self, json_str: &str,currtstmp:String) ->std::io::Result<Vec<String>>{
        self.log_message(&format!("{}-11-undo-{}",currtstmp, json_str))?;

        let result = self.undomain(json_str, &currtstmp); // delete last
        match &result {
            Ok(vec) => {}
                //println!("{}-{:?}", currtstmp, vec);
            //}
            Err(e) => {
                self.log_message(&format!("{}-{}",currtstmp, e));
            }
        } 
        self.log_message(&format!("{}-00-Done",currtstmp))?;

        result

    }

    pub fn update(&mut self, json_str: &str) ->std::io::Result<Vec<String>>{
        let currtstmp:String = self.current_timestamp();
        let result = self.updateWTS(json_str,currtstmp);
        result                 

    }

    pub fn updateWTS(&mut self, json_str: &str,currtstmp:String) ->std::io::Result<Vec<String>>{
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-11-update-{}",currtstmp, compact_jsonstr))?;

        let result =  self.updatemain(json_str, false,&currtstmp); // delete last
        match &result {
            Ok(vec) => {}
                //println!("{}-{:?}", currtstmp, vec);
            //}
            Err(e) => {
                self.log_message(&format!("{}-{}",currtstmp, e));
            }
        }         
        self.log_message(&format!("{}-00-Done", currtstmp));   
        result                 

    }    
    pub fn updateall(&mut self,json_str: &str) ->std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let result = self.updateallWTS(json_str,currtstmp);
        result  
    }

    pub fn updateallWTS(&mut self,json_str: &str,currtstmp:String) ->std::io::Result<Vec<String>> {
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-11-updateall-{}",currtstmp, compact_jsonstr))?;

       let result = self.updatemain(json_str, true,&currtstmp); // delete all
        match &result {
            Ok(vec) => {}
                //println!("{}-{:?}", currtstmp,vec);
            //}
            Err(e) => {
                self.log_message(&format!("{}-{}",currtstmp, e));
            }
        }        
       self.log_message(&format!("{}-00-Done", currtstmp));                    
       result

    }

    pub fn undomain(&mut self, json_str: &str,timestamp: &str) ->std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};

        // Step 1: Parse the input JSON string into a Value.
        let json: Value = serde_json::from_str(json_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Step 3: Validate JSON is an object
        if !json.is_object() {
            Error::new(ErrorKind::InvalidInput, 
                    "JSON input is not an object");

        }

        let file_name = json.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;

        //Step 4 A: Extract primary_key-value map from json
        let primkey_map: IndexMap<String, String> = self.extract_prim_keyname_value_map(&json)?;

        //get undo count
        let mut undocount: i64 = if let Some(n) = json.get("undocount").and_then(|v| v.as_i64()) {
            if n < 1 {
                return Err(Error::new(ErrorKind::InvalidInput, 
                    format!("'undocount' must be >= 1, got {}", n)));

            }
            n
        } else {
            1 // default
        };

        if(primkey_map.len() == 0){
            return Err(Error::new(ErrorKind::InvalidInput, 
                "Missing Primary Key : 'primkey', mandatory for undo"));
        }

        // Step 4 B: Extract key-value map from JSON
        let mut key_map: IndexMap<String, String> = primkey_map.clone();

        // Step 5: Get matching pointers
        let mut farthest_ptr: usize = 0;
        let mut restore_ptr: usize = 0;

        if let Some(ptrs) = self.get_fromkey_onlypointers(file_name, &key_map) {
            if(ptrs.len() > 1){
                //undo operation possible only when unique records can be identified
                //using key specified
                return Err(Error::new(ErrorKind::InvalidInput, 
                    "Multiple records found for given primkey fields, make sure to use unique key"));                
            }
            // collect into a mutable Vec (works whether ptrs is Vec or slice)
            //these are pointers to non-deleted records
            let mut sortedptrs: Vec<usize> = ptrs.iter().copied().collect();

            // sort descending by address
            sortedptrs.sort_by(|a, b| ((*b as usize).cmp(&(*a as usize))));
            farthest_ptr = sortedptrs[0].clone();
        }

        if let Some(deletedptrs) 
                = self.get_fromkey_onlypointersForDeleted(file_name, &key_map){

            if(deletedptrs.len() == 0){
                return Err(Error::new(ErrorKind::InvalidInput, 
                    "No deleted records exist, 'undo' not possible."));                   
            }        
            let mut sortedptrs: Vec<usize> = deletedptrs.iter().copied().collect();
            // sort descending by address
            sortedptrs.sort_by(|a, b| ((*b as usize).cmp(&(*a as usize))));

            if(sortedptrs.len() < undocount as usize){
                return Err(Error::new(ErrorKind::InvalidInput, 
                    format!("undocount: {} cannot be more than deleted records count: {}",undocount,sortedptrs.len())));   
            }

            let mut counter = 1;

            for deleteptr in &sortedptrs {
                if(counter == undocount){
                    restore_ptr = deleteptr.clone();
                    break;
                }
                else{
                    counter += 1;
                    }
                }
        }
        else{
            return Err(Error::new(ErrorKind::InvalidInput, 
                "No deleted records exist, 'undo' not possible."));                   
        }

            //we now have farthest_ptr -> pointer to be deleted
            //and restore_ptr -> pointer to be restored

            //get the line (string) to be restored
            let mut restorerecord: String = String::new();

            match self.print_line_forpointer_uselen(file_name, restore_ptr, true) {
                Ok(line) => {
                        restorerecord = line.clone();
                }
                Err(e) => {
                    return Err(Error::new(ErrorKind::InvalidInput, 
                        "No record found"));
                }
       
            }
            //exract json data
            let origjson = extract_json(&restorerecord,self.settings.recorddelimiter);
            //delete orig record
            let mut deletesuccess = true;
            let mut errmsg: String = "".to_string();

            if(farthest_ptr != 0){
                let mut farthest_ptr_vector = vec![farthest_ptr]; // v: Vec<usize> with one element
                self.log_message(&format!("{}-1-{}-D-{}-I", timestamp,farthest_ptr,restore_ptr))?;

                if let Err(e) = self.delete_using_pointerVector(file_name, 
                                        farthest_ptr_vector.clone(),
                                        "d".to_string(), 
                                        timestamp){
                    deletesuccess = false;      
                    errmsg = format!("{}",e);    
                }
            }
            else{
                self.log_message(&format!("{}-1-{}-T-I", timestamp,restore_ptr))?;
            }
            
            if(deletesuccess){
                self.insert_duplicate_frmObject(&origjson, timestamp);
            }


              
        let lines = vec![
            String::from("done")
        ];
        Ok(lines)
    }    


    pub fn updatemain(&mut self, json_str: &str, update_all: bool, timestamp: &str) ->std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};

        // Step 1: Parse the input JSON string into a Value.
        let json: Value = serde_json::from_str(json_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Step 3: Validate JSON is an object
        if !json.is_object() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "JSON input is not an object",
            ));
        }

        let file_name = json.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;
        
        let mut key_map: IndexMap<String, String> = IndexMap::new();
        
        //Step 4 A: Extract primary_key-value map from json

        if json.get("primkey").is_some(){
            key_map = self.extract_prim_keyname_value_map(&json)?;
        }

        if(key_map.len() == 0){
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing primKey; 'primkey' is required for update as it is used to filter the records.",
            ));            
        }


        let mut ptrs: Vec<usize> = Vec::new();
        if(update_all){
		    let (ptrs_match, lines)  
                = self.get_fromkey(file_name, 
                                    &key_map, 
                                    "0",
                                    "0", 
                                    false,
                                    false,
                                    false,
                                    0,
                                    1);

            ptrs = ptrs_match;

        }
        else{
		    let (ptrs_match, lines)  
                = self.get_fromkey(file_name, 
                                    &key_map, 
                                    "0",
                                    "0", 
                                    true,
                                    false,
                                    false,
                                    0,
                                    1);
            ptrs = ptrs_match;


        }
        let ptrsize = ptrs.len();

        if(ptrsize > 0){
            for ptr in &ptrs {
                self.log_message(&format!("{}-1-{}-T-D-I-{}", timestamp,ptr,ptrsize))?;

            }
            self.delete_using_pointerVector(file_name, ptrs.clone(),"u".to_string(), timestamp);

            for &ptr in &ptrs {
                let origrec = self.print_line_forpointer_uselen(file_name,ptr,true)?;
                //exract json data
                let origjson = extract_json(&origrec,self.settings.recorddelimiter);
                if(!origjson.is_null()){
                    let mut i_key_map: IndexMap<String, String> = self.extract_prim_keyname_value_map(&origjson)?;
                  
                    //primary key also cannot be changed
                    i_key_map.entry("primkey".to_string()).or_insert("".to_string());   
                    let key_set: HashSet<&str> = i_key_map.keys().map(|s| s.as_str()).collect();

                    let modjson: Value = json.clone();

                    //merge json (update json) with orig json data
                    let mergedjson = merge(&origjson, &modjson,&key_set);
                    //insert merged json 
                    self.insert_duplicate_frmObject(&mergedjson, timestamp);  

                }
            }
        }
        else{
            self.log_message(&format!("{}-0-0-D-I-{}", timestamp,"Record_not_found"))?;

        }
        let lines = vec![
            String::from("done")
        ];
        Ok(lines)
    }



    pub fn gethistall(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        if !self.settings.enableviewdelete {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied, // Use PermissionDenied or Other
                "EnableViewDelete feature not enabled.",
            ));
        }
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-gethistall-{}",currtstmp, compact_jsonstr))?;

       return self.getmain(&json_str, false,true,false);  
    }

    pub fn gethist(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        if !self.settings.enableviewdelete {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied, // Use PermissionDenied or Other
                "EnableViewDelete feature not enabled.",
            ));
        }
        
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-gethist-{}",currtstmp, compact_jsonstr))?;
    

       return self.getmain(&json_str, true,true,false);  
    }


    pub fn gethistalldesc(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        if !self.settings.enableviewdelete {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied, // Use PermissionDenied or Other
                "EnableViewDelete feature not enabled.",
            ));
        }
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-gethistalldesc-{}",currtstmp, compact_jsonstr))?;

       return self.getmain(&json_str, false,true,true);  
    }


    pub fn gethistdesc(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        if !self.settings.enableviewdelete {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied, // Use PermissionDenied or Other
                "EnableViewDelete feature not enabled.",
            ));
        }
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-gethistdesc-{}",currtstmp, compact_jsonstr))?;

       return self.getmain(&json_str, true,true,true);  
    }  

    pub fn get(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-get-{}",currtstmp, compact_jsonstr))?;

        return self.getmain(&json_str,  true,false,false);  
    }

    pub fn getall(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-getall-{}",currtstmp, compact_jsonstr))?;

       return self.getmain(&json_str, false,false,false);  
    }

    pub fn getdesc(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-getdesc-{}",currtstmp, compact_jsonstr))?;

        return self.getmain(&json_str,  true,false,true);  
    }
    
    pub fn getalldesc(&mut self, json_str: &str) -> std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-getalldesc-{}",currtstmp, compact_jsonstr))?;

       return self.getmain(&json_str, false,false,true);  
    }

    //apply AND between the keys of primary key
    //this do not check for recid id
    pub fn getmainforduplicatecheck(&mut self, 
                    json: &Value,
                    use_intersection: bool,
                    includedelete: bool,
                    pureprimarykey: bool) //do i need to include 'recid' to check duplicates
                    -> std::io::Result<Vec<usize>> {

        use std::io::{Error, ErrorKind};
        use serde_json::Value;

        // Ensure it's an object
        let obj = json.as_object().ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "Expected JSON object at root")
        })?;

        let file_name = obj.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;


        //Extract primary_key-value map from json
        let mut primkey_map: IndexMap<String, String> = self.extract_prim_keyname_value_map(&json)?;

        if(pureprimarykey){
            primkey_map.swap_remove("recid");
        }
        if(primkey_map.len() == 0){
            Error::new(ErrorKind::InvalidInput, 
                "Missing Primary Key : 'primkey', mandatory for insert");
        }

        
        //get key->keyvalue map
        // Call the existing method
        let lines = self.get_fromkey_forduplicate(file_name, &primkey_map, use_intersection,includedelete,false);
        //for line in &lines {
            //println!("Line: {}", line);
            //need to print just one record, no need to print all
            //break;
        //}
        Ok(lines)

    }  


    //apply AND between the keys
    pub fn getmain(&mut self, 
                    json_str: &str,
                    use_intersection: bool,
                    includedelete: bool,
                    reverse: bool,) -> std::io::Result<Vec<String>> {

        use std::io::{Error, ErrorKind};
        use serde_json::Value;

        // Parse the JSON string
        let json: Value = serde_json::from_str(json_str)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, format!("Invalid JSON: {}", e)))?;

        // Ensure it's an object
        let obj = json.as_object().ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "Expected JSON object at root")
        })?;

        let file_name = obj.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;
        
        //paging implementation for get
        let recstart = obj.get("recstart").and_then(Value::as_str).unwrap_or("0");
        //sortby
        let sortby = obj.get("sortbykey").and_then(Value::as_str).unwrap_or("0");

        //paging implementation for get
        let mut maxrecordscount: usize = obj
                                    .get("maxrecordscount")
                                    .and_then(Value::as_u64)        // → Option<u64>
                                    .map(|n| n as usize)            // → Option<usize>
                                    .unwrap_or(self.settings.maxgetrecords);   

        if(maxrecordscount > self.settings.maxgetrecords){
            maxrecordscount = self.settings.maxgetrecords;
        }                        


        //get key->keyvalue map
        let key_map = self.extract_keyname_value_map(&json)?;    
        // Call the existing method
        let (ptrs, lines)  
                = self.get_fromkey(file_name, &key_map, &recstart, &sortby, use_intersection,includedelete,reverse,maxrecordscount,2);
        Ok(lines)

    }

    pub fn get_fromkey_forduplicate(
        &mut self,
        file_name: &str,
        key_map: &IndexMap<String, String>,
        use_intersection: bool,
        includedelete: bool,
        reverse: bool, //currently ingnored
    ) -> Vec<usize> {

        let mut result: Vec<usize> = Vec::new(); // ✅ Flat vector

        if !use_intersection{
            let mut seen = FxHashSet::default();

            for (keyname, keyvalue) in key_map {
                if let Some(ptrmap) = self.file_key_pointer_map.get(file_name) {
                    if let Some(val_map) = ptrmap.get(keyname) {
                        if let Some(ptrs) = val_map.get(&NumKey(keyvalue.into()) ) {
                            for ptr in ptrs {
                                let key = ptr; // Use raw pointer for uniqueness
                                if seen.insert(key) {
                                    result.push(ptr.clone());
                                }
                            }
                        }
                    }
                }
            }

        }
        else{
            let mut is_first = true;

            for (keyname, keyvalue) in key_map {
                if let Some(ptrmap) = self.file_key_pointer_map.get(file_name) {
                    if let Some(val_map) = ptrmap.get(keyname) {
                        if let Some(ptrs) = val_map.get(&NumKey(keyvalue.into())) {
                            if ptrs.is_empty() {
                                result.clear(); // Found empty ptrs -> clear result
                                break;
                            }
                            if is_first {
                                result = ptrs.iter().cloned().collect();
                                is_first = false;
                            } else {
                                result.retain(|ptr| {
                                    ptrs.iter().any(|p| p == ptr)
                                });
                                if result.is_empty() {
                                    break; // Intersection became empty -> stop
                                }
                            }
                        } else {
                            result.clear(); // keyvalue missing -> clear result
                            break;
                        }
                    } else {
                        result.clear(); // keyname missing -> clear result
                        break;
                    }
                } else {
                    result.clear(); // ptrmap missing -> clear result
                    break;
                }
            }
        }
        return result;
    }


    /// Copy matching entries from `src` into `dst` according to `cond` ("<", "<=", ">", ">=").
    /// `dst` stores references; existing keys in `dst` are left as-is (ignored).
    pub fn filter_on_range_into_map_all<'a>(
        &self,
        cond: &str,
        src: &'a BTreeMap<NumKey, Vec<usize>>,
        dst: &mut BTreeMap<&'a NumKey, &'a Vec<usize>>,
    ) {
        let cond = cond.trim();

        // parse operator and key
        let (op, key_str) = if let Some(rest) = cond.strip_prefix(">=") {
            (">=", rest.trim())
        } else if let Some(rest) = cond.strip_prefix("<=") {
            ("<=", rest.trim())
        } else if let Some(rest) = cond.strip_prefix('>') {
            (">", rest.trim())
        } else if let Some(rest) = cond.strip_prefix('<') {
            ("<", rest.trim())
        } else {
            // empty key → exit gracefully
            // eprintln!("empty key after operator in condition: '{cond}'");
            return;
        };
        // Build a NumKey from your input once.
        let key: NumKey = NumKey(key_str.to_string());

        // iterate the matching range (works if NumKey supports borrowed lookup by `str`)
        let iter = match op {
            "<"  => src.range::<NumKey, _>((Unbounded, Excluded(key))),
            "<=" => src.range::<NumKey, _>((Unbounded, Included(key))),
            ">"  => src.range::<NumKey, _>((Excluded(key), Unbounded)),
            ">=" => src.range::<NumKey, _>((Included(key), Unbounded)),
            _    => unreachable!(),
        };

        // insert references; ignore if key already present
        for (k, v) in iter {
            dst.entry(k).or_insert(v);
        }
    }

    // Function to print the whole map
    fn print_whole_map(&self,ptrmap: &HashMap<String, BTreeMap<NumKey, Vec<usize>>>) {
        for (outer_key, inner_btree) in ptrmap {
            println!("Outer key: {}", outer_key);

            for (num_key, values) in inner_btree {
                println!("  {} → {:?}", num_key, values);
            }
        }
    }

    /// Keep in `dst` only those entries whose keys also fall in `src`'s range `cond`.
    /// `dst` stores references tied to `src` (read-only view).
    pub fn filter_on_range_into_map_intersect<'a>(
        &self,
        cond: &str,
        src: &'a BTreeMap<NumKey, Vec<usize>>,
        dst: &mut BTreeMap<&'a NumKey, &'a Vec<usize>>,
    ) {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        use std::collections::BTreeSet;

        let cond = cond.trim();

        let (op, key_str) = if let Some(rest) = cond.strip_prefix(">=") {
            (">=", rest.trim())
        } else if let Some(rest) = cond.strip_prefix("<=") {
            ("<=", rest.trim())
        } else if let Some(rest) = cond.strip_prefix('>') {
            (">", rest.trim())
        } else if let Some(rest) = cond.strip_prefix('<') {
            ("<", rest.trim())
        } else {
            //panic!("bad op in condition: '{cond}' (expected one of: >=, <=, >, <)");
            return;
        };

        // Build a NumKey from your input once.
        let key: NumKey = NumKey(key_str.to_string());

        // range query
        let src_iter = match op {
            "<"  => src.range::<NumKey, _>((Unbounded, Excluded(key))),
            "<=" => src.range::<NumKey, _>((Unbounded, Included(key))),
            ">"  => src.range::<NumKey, _>((Excluded(key), Unbounded)),
            ">=" => src.range::<NumKey, _>((Included(key), Unbounded)),
            _    => unreachable!(),
        };

        // collect references to keys directly
        let keep: BTreeSet<&'a NumKey> = src_iter.map(|(k, _)| k).collect();

        // retain only entries in keep
        dst.retain(|k, _| keep.contains(k));
    }

    /// Returns all record ids from `val_map` that satisfy the condition in `cond`,
    /// where `cond` is like ">=200", "<=400", ">", "<".
    /// `val_map` is assumed to be `BTreeMap<String, Vec<usize>>` (string-ordered keys).
    fn filterOnRange(&self,cond: &str, val_map: &BTreeMap<NumKey, Vec<usize>>) -> Vec<usize> {
        // Trim whitespace first
        let cond = cond.trim();

        // Detect operator, prioritizing two-char ops
        let (op, key_str) = if let Some(rest) = cond.strip_prefix(">=") {
            (">=", rest.trim())
        } else if let Some(rest) = cond.strip_prefix("<=") {
            ("<=", rest.trim())
        } else if let Some(rest) = cond.strip_prefix('>') {
            (">", rest.trim())
        } else if let Some(rest) = cond.strip_prefix('<') {
            ("<", rest.trim())
        } else {
            //panic!("bad op in condition: '{cond}' (expected one of: >=, <=, >, <)");
            return Vec::new();
        };

        if key_str.is_empty() {
            //panic!("empty key after operator in condition: '{cond}'");
             return Vec::new();
        }
        // Build a NumKey from your input once.
        let key: NumKey = NumKey(key_str.to_string());

        // Build the range iterator based on the operator
        let iter = match op {
            "<"  => val_map.range::<NumKey, _>((Unbounded, Excluded(key))),
            "<=" => val_map.range::<NumKey, _>((Unbounded, Included(key))),
            ">"  => val_map.range::<NumKey, _>((Excluded(key), Unbounded)),
            ">=" => val_map.range::<NumKey, _>((Included(key), Unbounded)),
            _    => unreachable!("op already validated"),
        };

        // Flatten all Vec<usize> values into a single Vec<usize>
        let result: Vec<usize> = iter.flat_map(|(_, v)| v.iter().copied()).collect();
        result

    }

    

    pub fn get_fromkey(
        &mut self,
        file_name: &str,
        key_map: &IndexMap<String, String>,
        recstart: &str, //if "0" send records from begining, else start from this record (including)
        sortby: &str, //'1' will indicate that results to be sorted by key; else value of field could also be mentioned
        use_intersection: bool,
        includedelete: bool,
        reverse: bool,   //send records in decending
        maxrecordscount: usize, //max number of records to return
        returnwhat:usize, //1->ponters; 2->lines; 3 ->pointers + lines
    ) -> (Vec<usize>, Vec<String>) {

        let mut result: Vec<usize> = Vec::new(); // ✅ Flat vector

        let mut found_keystart = recstart == "0";

        //create map of key/value containing functions
        // values that look like a function call, e.g. "max()" or "top(10)"

        let functionkeyRawmap: HashMap<String, String> = key_map
            .iter()
            .filter(|(_, v)| {
                let v = v.trim_end();
                v.ends_with(')') && v.contains('(')   // cheap function-ish check
            })
            .map(|(k, v)| (k.clone(), v.clone()))  // keep keys unchanged
            .collect();
        
        // make a set of normalized keys (remove trailing "_<char>")
        let functionkeyset: FxHashSet<String> = functionkeyRawmap
            .keys()
            .map(|k| {
                let mut s = k.clone();
                if let Some((last_start, _last_ch)) = s.char_indices().next_back() {
                    // if the byte immediately before the last char is '_', drop both
                    if last_start >= 1 && s.as_bytes()[last_start - 1] == b'_' {
                        s.truncate(last_start - 1); // remove '_' and the last char
                    }
                }
                s
            })
            .collect();


        let mut total_records = 0; // your value
        let mut result_lines: Vec<String> = Vec::with_capacity(self.settings.maxgetrecords + 1);
        

        let mut usefullmap = false;

        if(functionkeyRawmap.len() == key_map.len()){
            //implying there are no keys other then function keys
            //so use whole file_key_pointer_map instead of filter
            usefullmap = true;
        }

        if !use_intersection{ //e.g. getall /////// OR ///////
            let mut seen =  FxHashSet::default();;

            //if keyvalue contains function, key_map need to be processed differently
            //as for that a temp BTreeMap need to be created
            if(!usefullmap || includedelete){ //some keys that are not a function are mentioned
                for (keyname, keyvalue) in key_map {
                    if(keyvalue.ends_with(")")){ //keyvalue is a function, so ignore here
                        continue;
                    }
                    if let Some(ptrmap) = self.file_key_pointer_map.get(file_name) {
                        if let Some(val_map) = ptrmap.get(keyname) {

                            if(keyvalue.starts_with(&self.settings.notoperator)){
                                let newkeyvalue 
                                    = keyvalue.strip_prefix(&self.settings.notoperator).unwrap_or(keyvalue);
                                for (key1, vec1) in val_map {
                                    if key1 != &NumKey(newkeyvalue.into())  {
                                        for ptr1 in vec1 {
                                            //check if
                                            let rawptr1 = ptr1; // Use raw pointer for uniqueness
                                            if seen.insert(rawptr1) {
                                                result.push(ptr1.clone());
                                            }
                                        }
                                    }
                                }
                            }
                            else if(keyvalue.starts_with(">")
                                    ||
                                    keyvalue.starts_with("<")
                                    ||
                                    keyvalue.starts_with(">=")
                                    ||
                                    keyvalue.starts_with("<="))
                            {
                                let mut results_vec: Vec<usize> = Vec::new();
                                let count_pipe = keyvalue.matches('|').count();

                                match count_pipe {
                                    0 => {
                                        // No '|' → process directly
                                        results_vec = self.filterOnRange(keyvalue.trim(),val_map);
                                    }
                                    1 => {
                                        // Exactly one '|'
                                        let parts: Vec<&str> = keyvalue.split('|').collect();
                                        let mut results_vec1 = self.filterOnRange(parts[0].trim(),val_map);
                                        let mut results_vec2 =self.filterOnRange(parts[1].trim(),val_map);

                                        //find intersections of above
                                        results_vec = self.intersection_unique_unordered(&results_vec1, &results_vec2);

                                    }
                                    _ => {
                                        // More than one '|' → invalid format
                                        eprintln!("Error: '|' can appear only once in '{}'", keyvalue);
                                    }
                                }

                                //find union of results_vec and result
                                let mut seen: FxHashSet<_> = result.iter().copied().collect();
                                for val in results_vec {
                                    if seen.insert(val) { // insert returns false if already present
                                        result.push(val);
                                    }
                                }

                            }
                            else if(keyvalue.contains(",")){
                                for newkeyvalue in keyvalue.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                                    if let Some(ptrs) = val_map.get(&NumKey(newkeyvalue.into()) ) {
                                        for ptr in ptrs {
                                            let key = ptr; // Use raw pointer for uniqueness
                                            if seen.insert(key) {
                                                result.push(ptr.clone());
                                            }
                                        }
                                    }
                                }
                            }                        
                            else{
                                if let Some(ptrs) = val_map.get(&NumKey(keyvalue.into()) ) {
                                    for ptr in ptrs {
                                        let key = ptr; // Use raw pointer for uniqueness
                                        if seen.insert(key) {
                                            result.push(ptr.clone());
                                        }
                                    }
                                }
                            }

                        }
                    }
                }
                if(includedelete){ //get from file_key_pointer_fordeleted_map as well and merge
                    for (keyname, keyvalue) in key_map {
                        if(keyvalue.ends_with(")")){ //keyvalue is a function, so ignore here
                            continue;
                        }
                        if let Some(ptrmap) = self.file_key_pointer_fordeleted_map.get(file_name) {
                            if let Some(val_map) = ptrmap.get(keyname) {
                                if(keyvalue.starts_with(&self.settings.notoperator)){
                                    let newkeyvalue 
                                        = keyvalue.strip_prefix(&self.settings.notoperator).unwrap_or(keyvalue);
                                    for (key1, vec1) in val_map {
                                        if key1 != &NumKey(newkeyvalue.into())  {
                                            for ptr1 in vec1 {
                                                //check if
                                                let rawptr1 = ptr1; // Use raw pointer for uniqueness
                                                if seen.insert(rawptr1) {
                                                    result.push(ptr1.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                                else if(keyvalue.starts_with(">")
                                        ||
                                        keyvalue.starts_with("<")
                                        ||
                                        keyvalue.starts_with(">=")
                                        ||
                                        keyvalue.starts_with("<="))
                                {
                                    let mut results_vec: Vec<usize> = Vec::new();
                                    let count_pipe = keyvalue.matches('|').count();

                                    match count_pipe {
                                        0 => {
                                            // No '|' → process directly
                                            results_vec = self.filterOnRange(keyvalue.trim(),val_map);
                                        }
                                        1 => {
                                            // Exactly one '|'
                                            let parts: Vec<&str> = keyvalue.split('|').collect();
                                            let mut results_vec1 = self.filterOnRange(parts[0].trim(),val_map);
                                            let mut results_vec2 =self.filterOnRange(parts[1].trim(),val_map);

                                            //find intersections of above
                                            results_vec = self.intersection_unique_unordered(&results_vec1, &results_vec2);

                                        }
                                        _ => {
                                            // More than one '|' → invalid format
                                            eprintln!("Error: '|' can appear only once in '{}'", keyvalue);
                                        }
                                    }

                                    //find union of results_vec and result
                                    let mut seen: FxHashSet<_> = result.iter().copied().collect();
                                    for val in results_vec {
                                        if seen.insert(val) { // insert returns false if already present
                                            result.push(val);
                                        }
                                    }

                                }
                                else if(keyvalue.contains(",")){
                                    for newkeyvalue in keyvalue.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                                        if let Some(ptrs) = val_map.get(&NumKey(newkeyvalue.into()) ) {
                                            for ptr in ptrs {
                                                let key = ptr; // Use raw pointer for uniqueness
                                                if seen.insert(key) {
                                                    result.push(ptr.clone());
                                                }
                                            }
                                        }
                                    }
                                }                        
                                else{
                                    if let Some(ptrs) = val_map.get(&NumKey(keyvalue.into()) ) {
                                        for ptr in ptrs {
                                            let key = ptr; // Use raw pointer for uniqueness
                                            if seen.insert(key) {
                                                result.push(ptr.clone());
                                            }
                                        }
                                    }
                                }

                            }
                        }
                    }

                }
            }
			if(returnwhat == 1){
                //only pointers
                return (result,Vec::new());
            }            
            if(functionkeyRawmap.len() == 0 || includedelete){ //no functions, so display results as is

                total_records = result.len(); // your value

                if(reverse){
                    result.sort_by(|a, b| b.cmp(a));
                }
                else{
                    result.sort();
                }

                //add meta data
                let meta_json_str = format!(r#"{{"meta":{{"total_count":{}}}}}"#, total_records);
                result_lines.push(meta_json_str.clone());

                for ptr in result {

                    if let Ok(line) = self.print_line_forpointer_uselen(file_name,ptr,includedelete) {

                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }

                        if !found_keystart {
                            if line.starts_with(&format!("{}", recstart)) {
                                found_keystart = true;
                                if result_lines.len() < maxrecordscount {
                                    result_lines.push(line.to_string());
                                }
                            }
                        } else {
                                if result_lines.len() < maxrecordscount {
                                    result_lines.push(line.to_string());
                                }
                                else{
                                    break;
                                }
                        }
                    }
                } 

                //reset count
                result_lines[0] = format!(r#"{{"meta":{{"total_count":{}}}}}"#, total_records);

                if result_lines.is_empty() {
                    //eprintln!("No matching records found.");
                    result_lines = Vec::new();
                } 

            }
            else{
                //we now have ptr vector of all filtered records based on AND
                //to perform operations e.g. min;max;median; etc. we need to get keyvalue
                //for the matching keys from the records corresponding to the filtered record pointers


                //now get all the relevent keys for the records
                let functionkeycount = functionkeyRawmap.len();
                if(!usefullmap){  
                     let mut keydetailsmap: HashMap<String,SortedVec<NumKey>> = HashMap::new();
                    let key_lines: &BTreeMap<usize, usize> = match self.file_key_line_map.get(file_name) {
                        Some(m) => m,
                        None => {
                            eprintln!("file_name '{}' not found in file_key_line_map", file_name);
                            return (Vec::new(),Vec::new()); // function returns Vec<String>, so bail out safely
                        }
                    };

                        {
                            // fetch Arc<Mmap> once, under a short lock, then drop locks
                            let mmap_arc: Arc<Mmap> = {
                                let stateR = self.stateR.lock().unwrap();
                                let r = stateR.file_mmap_map_forread.read().unwrap();
                                match r.get(file_name).cloned() {
                                    Some(v) => v,
                                    None => {
                                        eprintln!("read mmap missing for file '{}'", file_name);
                                        return (Vec::new(),Vec::new()); // ← adjust to your function's actual return type
                                    }
                                }
                            };
                            let keystartbuffer = ((3 + TIMESTAMP_LEN + 1) * 2)
                                    + ((RECORD_SIZE_DIGITCOUNT) * 2) + (1 * 2)
                                    + (RECORDKEY_SIZE_DIGITCOUNT * 2) + (1 * 2); 

                            for ptr in result {
                                let mut functionkeyprocessed = 0;

                                let keystr_start =
                                    ptr + keystartbuffer;

                                // recorded key length for this record
                                let key_record_len: usize = match key_lines.get(&ptr) {
                                    Some(&len) => len,
                                    None => {
                                        eprintln!("ptr {} not found", ptr);
                                        return (Vec::new(),Vec::new()) // or return; if your function returns ()
                                    }
                                };

                                // ignore if record marked as deleted
                                if key_record_len >= ADD_TOMARK_REC_DELETE_IN_RECORDKEY {
                                    continue;
                                }

                                let keystr_end = keystr_start + key_record_len;
                                if keystr_end > mmap_arc.len() {
                                    // out-of-bounds safeguard; skip or handle as you prefer
                                    continue;
                                }

                                let keystr_slice = &mmap_arc[keystr_start..keystr_end];

                                let keystr_utf16: Vec<u16> = keystr_slice
                                    .chunks_exact(2)
                                    .map(|b| u16::from_le_bytes([b[0], b[1]]))
                                    .collect();

                                if let Ok(key_field) = String::from_utf16(&keystr_utf16) {
                                    for part in key_field.split(self.settings.indexdelimiter) {
                                        if let Some(dash_pos) = part.find(self.settings.indexnamevaluedelimiter) {
                                            let key_name = &part[..dash_pos];
                                            let key_value = &part[dash_pos + 1..];

                                            if functionkeyset.contains(key_name) {
                                                functionkeyprocessed += 1;
                                                keydetailsmap
                                                    .entry(key_name.to_string())
                                                    .or_insert_with(SortedVec::new)
                                                    .push(key_value.into());

                                                if functionkeyprocessed == functionkeycount {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    for (keyName, keyvalue) in functionkeyRawmap {
                        //functionkeyRawmap may have '_<letters' in case of multiple
                        //request for same field
                        //so remove '_<letter>' part
                        let mut key = keyName.clone();
                        if key.len() > 2 {
                            let n = key.len();
                            let bytes = key.as_bytes();
                            if bytes[n - 2] == b'_' {
                                key.truncate(n - 2);
                            }
                        }                        
                        if(keyvalue.starts_with("min()"))
                        {
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let msg = match inner.first() {
                                    Some(nk) => format!("{}->{}:{}",keyName,keyvalue, nk),   // nk: &NumKey
                                    None => format!("No Matching Records Found for: {}->{}",keyName,keyvalue).to_string(),
                                };
                                result_lines.push(msg);
                            }        

                        }
                        else if(keyvalue.starts_with("max()"))
                        {
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let msg = match inner.last() {
                                    Some(nk) => format!("{}->{}:{}",keyName,keyvalue, nk),   // nk: &NumKey
                                    None => format!("No Matching Records Found for: {}->{}",keyName,keyvalue).to_string(),
                                };
                                result_lines.push(msg);
                            }        

                        }
                        else if(keyvalue.starts_with("top("))
                        {
                            let top_n =     keyvalue.split_once('(')
                                            .and_then(|(_, r)| r.split_once(')'))
                                            .map(|(inside, _)| inside.trim().parse::<usize>().ok())
                                            .flatten()
                                            .unwrap_or(1);

                            if let Some(inner) = keydetailsmap.get(&key) {
                                let results: Vec<String> = inner
                                    .iter()
                                    .rev()                 // iterate from highest (end) to lowest
                                    .take(top_n)           // only take top_n
                                    .map(|nk| nk.to_string()) // requires NumKey: Display (or use format!("{:?}", nk))
                                    .collect();

                                if(results.len() == 0){
                                    let msg = format!("No Matching Records Found for: {}->{}", keyName, keyvalue);
                                    result_lines.push(msg);  
                                }    
                                let mut reccounter = 1;
                                for nk in results {
                                    let msg = format!("{}->{}:{}",keyName,reccounter, nk);
                                    result_lines.push(msg);  
                                    reccounter += 1;
                                    if(reccounter > self.settings.maxgetrecords){
                                        break;
                                    }

                                }
                         

                            }        

                        }
                        else if(keyvalue.starts_with("last("))
                        {
                            let last_n =     keyvalue.split_once('(')
                                            .and_then(|(_, r)| r.split_once(')'))
                                            .map(|(inside, _)| inside.trim().parse::<usize>().ok())
                                            .flatten()
                                            .unwrap_or(1);

                            if let Some(inner) = keydetailsmap.get(&key) {
                                let results: Vec<String> = inner
                                    .iter()
                                    .take(last_n)           // only take top_n
                                    .map(|nk| nk.to_string()) // requires NumKey: Display (or use format!("{:?}", nk))
                                    .collect();

                                if(results.len() == 0){
                                    let msg = format!("No Matching Records Found for: {}->{}", keyName, keyvalue);
                                    result_lines.push(msg);  
                                }   
                                let mut reccounter = 1;
                                for nk in results {
                                    let msg = format!("{}->{}:{}",keyName,reccounter, nk);
                                    result_lines.push(msg);  
                                    reccounter += 1;

                                    if(reccounter >= self.settings.maxgetrecords){
                                        break;
                                    }
                                }                           
                            }        

                        }
                        else if(keyvalue.starts_with("mean("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.mean_from_numkey_slice(&inner);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;
                                result_lines.push(msg);
                            } 

                        }    
                        else if(keyvalue.starts_with("mode("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.mode_from_numkey_vec(&inner);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("variancePopulation("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.variance_from_numkey_vec(&inner,true);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("varianceSample("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.variance_from_numkey_vec(&inner,false);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("stddevPopulation("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.stddev_from_numkey_vec(&inner,true);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("stddevSample("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.stddev_from_numkey_vec(&inner,false);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("percentile_nearest_rank("))
                        {   
                            let val: usize = keyvalue.find('(')
                                .and_then(|i| keyvalue[i + 1..].find(')')
                                    .map(|j| &keyvalue[i + 1..i + 1 + j]))
                                .and_then(|s| s.parse::<usize>().ok())
                                .unwrap_or(0); // fallback if parsing fails

                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.percentile_nearest_rank_from_vec(&inner,val as f64);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("percentile_linear("))
                        {   
                            let val: usize = keyvalue.find('(')
                                .and_then(|i| keyvalue[i + 1..].find(')')
                                    .map(|j| &keyvalue[i + 1..i + 1 + j]))
                                .and_then(|s| s.parse::<usize>().ok())
                                .unwrap_or(0); // fallback if parsing fails                        
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.percentile_linear_from_vec(&inner,val as f64);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("median("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.median_from_numkey_vec(&inner);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        }                                                                                                                                                                                                         

                    }                    
                }
                else{
                    //functions need to be applied to whole database
                    if let Some(filtered_map_file) = self.file_key_pointer_map.get(file_name) {
                        for (keyName, keyvalue) in &functionkeyRawmap {
                            //functionkeyRawmap may have '_<letters' in case of multiple
                            //request for same field
                            //so remove '_<letter>' part
                            let mut key = keyName.clone();
                            if key.len() > 2 {
                                let n = key.len();
                                let bytes = key.as_bytes();
                                if bytes[n - 2] == b'_' {
                                    key.truncate(n - 2);
                                }
                            }
                            if(keyvalue.starts_with("min()"))
                            {
                                if let Some(inner) = filtered_map_file.get(&key) {
                                    if let Some((_min_key, last_vals)) = inner.iter().next() {
                                        let msg = format!("{}->{}:{}",keyName,keyvalue, _min_key);
                                        result_lines.push(msg);                                
                                    }
                                }        

                            }
                            else if(keyvalue.starts_with("max()"))
                            {
                                if let Some(inner) = filtered_map_file.get(&key) {
                                    if let Some((_max_key, last_vals)) = inner.iter().next_back() {
                                        let msg = format!("{}->{}:{}",keyName,keyvalue, _max_key);
                                        result_lines.push(msg);  
                                    }
                                }                            

                            }                    
                            else if(keyvalue.starts_with("last("))
                            {   
                                
                                let last_n =     keyvalue.split_once('(')
                                                .and_then(|(_, r)| r.split_once(')'))
                                                .map(|(inside, _)| inside.trim().parse::<usize>().ok())
                                                .flatten()
                                                .unwrap_or(1);
                                if let Some(inner) = filtered_map_file.get(&key) {
                                    let mut reccounter = 1;
                                    for (nk, v) in inner.iter().take(last_n) {
                                        let msg = format!("{}->{}:{}",keyName, reccounter,nk.to_string());
                                        result_lines.push(msg);  
                                        reccounter += 1;

                                        if(reccounter >= self.settings.maxgetrecords){
                                            break;
                                        }                                        
                                    }
                                }
                            }
                            else if(keyvalue.starts_with("top("))
                            {   
                                
                                let top_n =     keyvalue.split_once('(')
                                                .and_then(|(_, r)| r.split_once(')'))
                                                .map(|(inside, _)| inside.trim().parse::<usize>().ok())
                                                .flatten()
                                                .unwrap_or(1);
        
                                if let Some(inner) = filtered_map_file.get(&key) {
                                    let mut reccounter = 1;
                                    for (nk, v) in inner.iter().rev().take(top_n) {
                                        let msg = format!("{}->{}:{}",keyName,reccounter, nk.to_string());
                                        result_lines.push(msg); 
                                        reccounter += 1;

                                        if(reccounter >= self.settings.maxgetrecords){
                                            break;
                                        }                                            
                                    }
                                }

                            }
                            else if(keyvalue.starts_with("mean("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.mean_from_btreemapptr(inner);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            }                   
                            else if(keyvalue.starts_with("mode("))
                            {   

                                if let Some(inner) = filtered_map_file.get(&key) {
                                    let mut resultstr = "".to_string();
                                    if let Some((mn, cnt)) = self.mode_frm_btree_mapptr(&inner) {
                                        resultstr = format!("{}->{}:{} ; Records Count: {}",keyName,keyvalue, mn, cnt);
                                    } else {
                                        resultstr = "No mode (empty input)".to_string();
                                    } 
                                    result_lines.push(resultstr);
                                }
                            }                     
                            else if(keyvalue.starts_with("median("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.median_from_btreemapptr(&inner);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("variancePopulation("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.variance_from_btreemapptr(&inner,true);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("varianceSample("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.variance_from_btreemapptr(&inner,false);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("stddevPopulation("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.stddevptr(&inner,true);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("stddevSample("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.stddevptr(&inner,false);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("percentile_nearest_rank("))
                            {   
                                let val: usize = keyvalue.find('(')
                                    .and_then(|i| keyvalue[i + 1..].find(')')
                                        .map(|j| &keyvalue[i + 1..i + 1 + j]))
                                    .and_then(|s| s.parse::<usize>().ok())
                                    .unwrap_or(0); // fallback if parsing fails
                                if let Some(inner) = filtered_map_file.get(&key) {


                                    let mn  = self.percentile_nearest_rank_from_btreeptr(&inner,val as f64);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("percentile_linear("))
                            {   
                                let val: usize = keyvalue.find('(')
                                    .and_then(|i| keyvalue[i + 1..].find(')')
                                        .map(|j| &keyvalue[i + 1..i + 1 + j]))
                                    .and_then(|s| s.parse::<usize>().ok())
                                    .unwrap_or(0); // fallback if parsing fails
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.percentile_linear_from_btreeptr(&inner,val as f64);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            }
                        }                     
                    }                

                }
            }               

        }
        else{ ///////////////////////////// Intersection : AND ////////////////////////
            let mut is_first = true;
            if let Some(ptrmap) = self.file_key_pointer_map.get(file_name) {
                for (keyname, keyvalue) in key_map {
                    if(keyvalue.ends_with(")")){
                        continue;
                    }
                    if let Some(val_map) = ptrmap.get(keyname) {

                        if(keyvalue.starts_with(&self.settings.notoperator)){
                            let newkeyvalue 
                                = keyvalue.strip_prefix(&self.settings.notoperator).unwrap_or(keyvalue);
                            
                            let mut ptrsfound =false;
                            let mut results_vec: Vec<usize> = Vec::new();                            
                            for (key1, vec1) in val_map {
                                if key1 != &NumKey(newkeyvalue.into()) {
                                    results_vec.extend(vec1);
                                    ptrsfound = true;  
                                }                               

                            }
                            if(!ptrsfound){
                                result.clear();
                            }


                            if is_first {
                                result = results_vec.iter().cloned().collect();
                                is_first = false;
                            } else {
                                if(sortby == keyname){
                                    result = self.intersection_unique_ordered(&results_vec, &result);
                                }
                                else{
                                    result = self.intersection_unique_ordered( &result, &results_vec);
                                }

                            } 
                            if result.is_empty() {
                                break; // Intersection became empty -> stop
                            }                                                       

                        }
                        else if(keyvalue.starts_with(">")
                                ||
                                keyvalue.starts_with("<")
                                ||
                                keyvalue.starts_with(">=")
                                ||
                                keyvalue.starts_with("<="))
                        {
                            let mut results_vec: Vec<usize> = Vec::new();
                            let count_pipe = keyvalue.matches('|').count();

                            match count_pipe {
                                0 => {
                                    // No '|' → process directly
                                    results_vec = self.filterOnRange(keyvalue.trim(),val_map);

                                }
                                1 => {
                                    // Exactly one '|'
                                    let parts: Vec<&str> = keyvalue.split('|').collect();
                                    let mut results_vec1 = self.filterOnRange(parts[0].trim(),val_map);
                                    let mut results_vec2 =self.filterOnRange(parts[1].trim(),val_map);

                                    //find intersections of above
                                    results_vec = self.intersection_unique_ordered(&results_vec1, &results_vec2);

                                }
                                _ => {
                                    // More than one '|' → invalid format
                                    eprintln!("Error: '|' can appear only once in '{}'", keyvalue);
                                }
                            }

                            //now find intersection
                            if is_first {
                                result = results_vec.clone();
                                is_first = false;
                            }
                            else{
                                if(sortby == keyname){
                                    result = self.intersection_unique_ordered(&results_vec, &result);
                                }
                                else{
                                    result = self.intersection_unique_ordered( &result, &results_vec);
                                }
                            }


                        }
                        else if(keyvalue.contains(",")){
                            let mut seen =  FxHashSet::default();
                            let mut resultForOr: Vec<usize> = Vec::new(); // ✅ Flat vector
                            for newkeyvalue in keyvalue.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                                if let Some(ptrs) = val_map.get(&NumKey(newkeyvalue.into()) ) {
                                    for ptr in ptrs {
                                        let key = ptr; // Use raw pointer for uniqueness
                                        if seen.insert(key) {
                                            resultForOr.push(ptr.clone());
                                        }
                                    }
                                }
                            }
                            //now find intersection
                            if is_first {
                                result = resultForOr.clone();
                                is_first = false;
                            }
                            else{
                                if(sortby == keyname){
                                    result = self.intersection_unique_ordered(&resultForOr, &result);
                                }
                                else{
                                    result = self.intersection_unique_ordered( &result, &resultForOr);
                                }
                            }                             
                        }
                        else if let Some(ptrs) = val_map.get(&NumKey(keyvalue.into()) ) {
                            if ptrs.is_empty() {
                                result.clear(); // Found empty ptrs -> clear result
                                break;
                            }
                            if is_first {
                                result = ptrs.iter().cloned().collect();
                                is_first = false;
                            } else {
                                if(sortby == keyname){
                                    result = self.intersection_unique_ordered(&ptrs, &result);
                                }
                                else{
                                    result = self.intersection_unique_ordered( &result, &ptrs);
                                }
                                if result.is_empty() {
                                    break; // Intersection became empty -> stop
                                }
                            }
                        } else {
                            result.clear(); // keyvalue missing -> clear result
                            break;
                        }
                    } else {
                        result.clear(); // keyname missing -> clear result
                        break;
                    }
                }
            }

            if(includedelete){
                let mut result_forDelete: Vec<usize> = Vec::new(); // ✅ Flat vector
                is_first = true;
                if let Some(ptrmap) = self.file_key_pointer_fordeleted_map.get(file_name) {
                    for (keyname, keyvalue) in key_map {
                        if(keyvalue.ends_with(")")){
                            continue;
                        }
                        if let Some(val_map) = ptrmap.get(keyname) {

                            if(keyvalue.starts_with(&self.settings.notoperator)){
                                let newkeyvalue 
                                    = keyvalue.strip_prefix(&self.settings.notoperator).unwrap_or(keyvalue);
                                
                                let mut ptrsfound =false;
                                for (key1, vec1) in val_map {

                                    if key1 != &NumKey(newkeyvalue.into()) {
                                        if is_first {
                                            result_forDelete = vec1.iter().cloned().collect();
                                            is_first = false;
                                        } else {
                                            result_forDelete.retain(|ptr| {
                                                vec1.iter().any(|p| p == ptr)
                                            });

                                        }
                                        ptrsfound = true;  
                                    }                               

                                }
                                if(!ptrsfound){
                                    result_forDelete.clear();
                                }
                                if result_forDelete.is_empty() {
                                    break; // Intersection became empty -> stop
                                }

                            }
                            else if(keyvalue.starts_with(">")
                                    ||
                                    keyvalue.starts_with("<")
                                    ||
                                    keyvalue.starts_with(">=")
                                    ||
                                    keyvalue.starts_with("<="))
                            {
                                let mut results_vec: Vec<usize> = Vec::new();
                                let count_pipe = keyvalue.matches('|').count();

                                match count_pipe {
                                    0 => {
                                        // No '|' → process directly
                                        results_vec = self.filterOnRange(keyvalue.trim(),val_map);
                                    }
                                    1 => {
                                        // Exactly one '|'
                                        let parts: Vec<&str> = keyvalue.split('|').collect();
                                        let mut results_vec1 = self.filterOnRange(parts[0].trim(),val_map);
                                        let mut results_vec2 =self.filterOnRange(parts[1].trim(),val_map);

                                        //find intersections of above
                                        results_vec = self.intersection_unique_unordered(&results_vec1, &results_vec2);

                                    }
                                    _ => {
                                        // More than one '|' → invalid format
                                        eprintln!("Error: '|' can appear only once in '{}'", keyvalue);
                                    }
                                }

                                //now find intersection
                                if is_first {
                                    result_forDelete = results_vec.clone();
                                    is_first = false;
                                }
                                else{
                                    result_forDelete = self.intersection_unique_unordered(&results_vec, &result_forDelete);
                                } 

                            }
                            else if(keyvalue.contains(",")){
                                let mut seen =  FxHashSet::default();
                                let mut resultForOr: Vec<usize> = Vec::new(); // ✅ Flat vector
                                for newkeyvalue in keyvalue.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                                    if let Some(ptrs) = val_map.get(&NumKey(newkeyvalue.into()) ) {
                                        for ptr in ptrs {
                                            let key = ptr; // Use raw pointer for uniqueness
                                            if seen.insert(key) {
                                                resultForOr.push(ptr.clone());
                                            }
                                        }
                                    }
                                }
                                //now find intersection
                                if is_first {
                                    result_forDelete = resultForOr.clone();
                                    is_first = false;
                                }
                                else{
                                    result_forDelete = self.intersection_unique_unordered(&resultForOr, &result_forDelete);
                                }                             
                            }
                            else if let Some(ptrs) = val_map.get(&NumKey(keyvalue.into()) ) {
                                if ptrs.is_empty() {
                                    result_forDelete.clear(); // Found empty ptrs -> clear result
                                    break;
                                }
                                if is_first {
                                    result_forDelete = ptrs.iter().cloned().collect();
                                    is_first = false;
                                } else {
                                    result_forDelete.retain(|ptr| {
                                        ptrs.iter().any(|p| p == ptr)
                                    });
                                    if result_forDelete.is_empty() {
                                        break; // Intersection became empty -> stop
                                    }
                                }
                            } else {
                                result_forDelete.clear(); // keyvalue missing -> clear result
                                break;
                            }
                        } else {
                            result_forDelete.clear(); // keyname missing -> clear result
                            break;
                        }
                    }
                }
                //merge 
                result.extend_from_slice(&result_forDelete);                
            }
			if(returnwhat == 1){
                //only pointers
                return (result,Vec::new());
            } 

            if(functionkeyRawmap.len() == 0 || includedelete){ //no functions, so display results as is
                total_records = result.len(); // your value

                if(sortby == "0"){ //no sortby field mentioned, so sort by insert datetime
                    if(reverse){
                        result.sort_by(|a, b| b.cmp(a));
                    }
                    else{
                        result.sort();
                    }
                }
                else{
                    if(reverse){
                        result.reverse();
                    }                    
                }


                //add meta data
                let meta_json_str = format!(r#"{{"meta":{{"total_count":{}}}}}"#, total_records);
                result_lines.push(meta_json_str.clone());

                for ptr in result {

                    if let Ok(line) = self.print_line_forpointer_uselen(file_name,ptr,includedelete) {

                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }

                        if !found_keystart {
                            if line.starts_with(&format!("{}", recstart)) {
                                found_keystart = true;
                                if result_lines.len() < maxrecordscount {
                                    result_lines.push(line.to_string());
                                }
                            }
                        } else {
                                if result_lines.len() < maxrecordscount {
                                    result_lines.push(line.to_string());
                                }
                                else{
                                    break;
                                }
                        }
                    }
                } 

                //reset count
                result_lines[0] = format!(r#"{{"meta":{{"total_count":{}}}}}"#, total_records);

                if result_lines.is_empty() {
                    //eprintln!("No matching records found.");
                    result_lines = Vec::new();
                } 

            }
            else{
                //we now have ptr vector of all filtered records based on AND
                //to perform operations e.g. min;max;median; etc. we need to get keyvalue
                //for the matching keys from the records corresponding to the filtered record pointers


                //now get all the relevent keys for the records
                let functionkeycount = functionkeyRawmap.len();
                if(!usefullmap){  
                    let mut keydetailsmap: HashMap<String,SortedVec<NumKey>> = HashMap::new();
                    let key_lines: &BTreeMap<usize, usize> = match self.file_key_line_map.get(file_name) {
                        Some(m) => m,
                        None => {
                            eprintln!("file_name '{}' not found in file_key_line_map", file_name);
                            return (Vec::new(),Vec::new()); // or `return;` if your fn returns ()
                        }
                    };

                        {
                            // Fetch Arc<Mmap> under a short lock, then drop all locks
                            let mmap_arc = {
                                let stateR = self.stateR.lock().unwrap();
                                let r = stateR.file_mmap_map_forread.read().unwrap();
                                match r.get(file_name).cloned() {
                                    Some(v) => v,
                                    None => {
                                        eprintln!("read mmap missing for file '{}'", file_name);
                                        return (Vec::new(),Vec::new()); // or `return;` if fn returns ()
                                    }
                                }
                            };
                            let keystartbuffer = ((3 + TIMESTAMP_LEN + 1) * 2)
                                    + ((RECORD_SIZE_DIGITCOUNT) * 2) + (1 * 2)
                                    + (RECORDKEY_SIZE_DIGITCOUNT * 2) + (1 * 2);

                            for ptr in result {
                                let mut functionkeyprocessed = 0;

                                let keystr_start =
                                    ptr + keystartbuffer;

                                let key_record_len = match key_lines.get(&ptr) {
                                    Some(&len) => len,
                                    None => {
                                        eprintln!("ptr {} not found", ptr);
                                        return (Vec::new(),Vec::new()); // or `return;` if your fn returns ()
                                    }
                                };

                                // ignore if record marked as deleted
                                if key_record_len >= ADD_TOMARK_REC_DELETE_IN_RECORDKEY {
                                    continue;
                                }

                                let keystr_end = keystr_start + key_record_len;
                                if keystr_end > mmap_arc.len() {
                                    // out-of-bounds safeguard
                                    continue;
                                }

                                let keystr_slice = &mmap_arc[keystr_start..keystr_end];

                                let keystr_utf16: Vec<u16> = keystr_slice
                                    .chunks_exact(2)
                                    .map(|b| u16::from_le_bytes([b[0], b[1]]))
                                    .collect();

                                if let Ok(key_field) = String::from_utf16(&keystr_utf16) {
                                    for part in key_field.split(self.settings.indexdelimiter) {
                                        if let Some(dash_pos) = part.find(self.settings.indexnamevaluedelimiter) {
                                            let key_name = &part[..dash_pos];
                                            let key_value = &part[dash_pos + 1..];

                                            if functionkeyset.contains(key_name) {
                                                functionkeyprocessed += 1;
                                                keydetailsmap
                                                    .entry(key_name.to_string())
                                                    .or_insert_with(SortedVec::new)
                                                    .push(key_value.into());
                                                if functionkeyprocessed == functionkeycount {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    for (keyName, keyvalue) in functionkeyRawmap {
                        //functionkeyRawmap may have '_<letters' in case of multiple
                        //request for same field
                        //so remove '_<letter>' part
                        let mut key = keyName.clone();
                        if key.len() > 2 {
                            let n = key.len();
                            let bytes = key.as_bytes();
                            if bytes[n - 2] == b'_' {
                                key.truncate(n - 2);
                            }
                        }                        
                        if(keyvalue.starts_with("min()"))
                        {
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let msg = match inner.first() {
                                    Some(nk) => format!("{}->{}:{}",keyName,keyvalue, nk),   // nk: &NumKey
                                    None => format!("No Matching Records Found for: {}->{}",keyName,keyvalue).to_string(),
                                };
                                result_lines.push(msg);
                            }        

                        }
                        else if(keyvalue.starts_with("max()"))
                        {
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let msg = match inner.last() {
                                    Some(nk) => format!("{}->{}:{}",keyName,keyvalue, nk),   // nk: &NumKey
                                    None => format!("No Matching Records Found for: {}->{}",keyName,keyvalue).to_string(),
                                };
                                result_lines.push(msg);
                            }        

                        }
                        else if(keyvalue.starts_with("top("))
                        {
                            let top_n =     keyvalue.split_once('(')
                                            .and_then(|(_, r)| r.split_once(')'))
                                            .map(|(inside, _)| inside.trim().parse::<usize>().ok())
                                            .flatten()
                                            .unwrap_or(1);

                            if let Some(inner) = keydetailsmap.get(&key) {
                                let results: Vec<String> = inner
                                    .iter()
                                    .rev()                 // iterate from highest (end) to lowest
                                    .take(top_n)           // only take top_n
                                    .map(|nk| nk.to_string()) // requires NumKey: Display (or use format!("{:?}", nk))
                                    .collect();

                                if(results.len() == 0){
                                    let msg = format!("No Matching Records Found for: {}->{}", keyName, keyvalue);
                                    result_lines.push(msg);  
                                }    
                                let mut reccounter = 1;
                                for nk in results {
                                    let msg = format!("{}->{}:{}",keyName,reccounter, nk);
                                    result_lines.push(msg);  
                                    reccounter += 1;
                                    if(reccounter > self.settings.maxgetrecords){
                                        break;
                                    }

                                }
                         

                            }        

                        }
                        else if(keyvalue.starts_with("last("))
                        {
                            let last_n =     keyvalue.split_once('(')
                                            .and_then(|(_, r)| r.split_once(')'))
                                            .map(|(inside, _)| inside.trim().parse::<usize>().ok())
                                            .flatten()
                                            .unwrap_or(1);

                            if let Some(inner) = keydetailsmap.get(&key) {
                                let results: Vec<String> = inner
                                    .iter()
                                    .take(last_n)           // only take top_n
                                    .map(|nk| nk.to_string()) // requires NumKey: Display (or use format!("{:?}", nk))
                                    .collect();

                                if(results.len() == 0){
                                    let msg = format!("No Matching Records Found for: {}->{}", keyName, keyvalue);
                                    result_lines.push(msg);  
                                }   
                                let mut reccounter = 1;
                                for nk in results {
                                    let msg = format!("{}->{}:{}",keyName,reccounter, nk);
                                    result_lines.push(msg);  
                                    reccounter += 1;

                                    if(reccounter >= self.settings.maxgetrecords){
                                        break;
                                    }
                                }                           
                            }        

                        }
                        else if(keyvalue.starts_with("mean("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.mean_from_numkey_slice(&inner);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;
                                result_lines.push(msg);
                            } 

                        }    
                        else if(keyvalue.starts_with("mode("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.mode_from_numkey_vec(&inner);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("variancePopulation("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.variance_from_numkey_vec(&inner,true);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("varianceSample("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.variance_from_numkey_vec(&inner,false);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("stddevPopulation("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.stddev_from_numkey_vec(&inner,true);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("stddevSample("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.stddev_from_numkey_vec(&inner,false);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("percentile_nearest_rank("))
                        {   
                            let val: usize = keyvalue.find('(')
                                .and_then(|i| keyvalue[i + 1..].find(')')
                                    .map(|j| &keyvalue[i + 1..i + 1 + j]))
                                .and_then(|s| s.parse::<usize>().ok())
                                .unwrap_or(0); // fallback if parsing fails

                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.percentile_nearest_rank_from_vec(&inner,val as f64);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("percentile_linear("))
                        {   
                            let val: usize = keyvalue.find('(')
                                .and_then(|i| keyvalue[i + 1..].find(')')
                                    .map(|j| &keyvalue[i + 1..i + 1 + j]))
                                .and_then(|s| s.parse::<usize>().ok())
                                .unwrap_or(0); // fallback if parsing fails                        
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.percentile_linear_from_vec(&inner,val as f64);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        } 
                        else if(keyvalue.starts_with("median("))
                        {   
                            if let Some(inner) = keydetailsmap.get(&key) {

                                let mn  = self.median_from_numkey_vec(&inner);
                                let msg = format!("{}->{}:{}",keyName,keyvalue, mn);   // nk: &NumKey;

                                result_lines.push(msg);
                            } 

                        }                                                                                                                                                                                                         

                    }                    
                }
                else{
                    //functions need to be applied to whole database
                    if let Some(filtered_map_file) = self.file_key_pointer_map.get(file_name) {
                        for (keyName, keyvalue) in &functionkeyRawmap {
                            //functionkeyRawmap may have '_<letters' in case of multiple
                            //request for same field
                            //so remove '_<letter>' part
                            let mut key = keyName.clone();
                            if key.len() > 2 {
                                let n = key.len();
                                let bytes = key.as_bytes();
                                if bytes[n - 2] == b'_' {
                                    key.truncate(n - 2);
                                }
                            }
                            if(keyvalue.starts_with("min()"))
                            {
                                if let Some(inner) = filtered_map_file.get(&key) {
                                    if let Some((_min_key, last_vals)) = inner.iter().next() {
                                        let msg = format!("{}->{}:{}",keyName,keyvalue, _min_key);
                                        result_lines.push(msg);                                
                                    }
                                }        

                            }
                            else if(keyvalue.starts_with("max()"))
                            {
                                if let Some(inner) = filtered_map_file.get(&key) {
                                    if let Some((_max_key, last_vals)) = inner.iter().next_back() {
                                        let msg = format!("{}->{}:{}",keyName,keyvalue, _max_key);
                                        result_lines.push(msg);  
                                    }
                                }                            

                            }                    
                            else if(keyvalue.starts_with("last("))
                            {   
                                
                                let last_n =     keyvalue.split_once('(')
                                                .and_then(|(_, r)| r.split_once(')'))
                                                .map(|(inside, _)| inside.trim().parse::<usize>().ok())
                                                .flatten()
                                                .unwrap_or(1);
                                if let Some(inner) = filtered_map_file.get(&key) {
                                    let mut reccounter = 1;
                                    for (nk, v) in inner.iter().take(last_n) {
                                        let msg = format!("{}->{}:{}",keyName, reccounter,nk.to_string());
                                        result_lines.push(msg);  
                                        reccounter += 1;

                                        if(reccounter >= self.settings.maxgetrecords){
                                            break;
                                        }                                        
                                    }
                                }
                            }
                            else if(keyvalue.starts_with("top("))
                            {   
                                
                                let top_n =     keyvalue.split_once('(')
                                                .and_then(|(_, r)| r.split_once(')'))
                                                .map(|(inside, _)| inside.trim().parse::<usize>().ok())
                                                .flatten()
                                                .unwrap_or(1);
        
                                if let Some(inner) = filtered_map_file.get(&key) {
                                    let mut reccounter = 1;
                                    for (nk, v) in inner.iter().rev().take(top_n) {
                                        let msg = format!("{}->{}:{}",keyName,reccounter, nk.to_string());
                                        result_lines.push(msg); 
                                        reccounter += 1;

                                        if(reccounter >= self.settings.maxgetrecords){
                                            break;
                                        }                                            
                                    }
                                }

                            }
                            else if(keyvalue.starts_with("mean("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.mean_from_btreemapptr(inner);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            }                   
                            else if(keyvalue.starts_with("mode("))
                            {   

                                if let Some(inner) = filtered_map_file.get(&key) {
                                    let mut resultstr = "".to_string();
                                    if let Some((mn, cnt)) = self.mode_frm_btree_mapptr(&inner) {
                                        resultstr = format!("{}->{}:{} ; Records Count: {}",keyName,keyvalue, mn, cnt);
                                    } else {
                                        resultstr = "No mode (empty input)".to_string();
                                    } 
                                    result_lines.push(resultstr);
                                }
                            }                     
                            else if(keyvalue.starts_with("median("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.median_from_btreemapptr(&inner);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("variancePopulation("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.variance_from_btreemapptr(&inner,true);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("varianceSample("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.variance_from_btreemapptr(&inner,false);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("stddevPopulation("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.stddevptr(&inner,true);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("stddevSample("))
                            {   
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.stddevptr(&inner,false);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("percentile_nearest_rank("))
                            {   
                                let val: usize = keyvalue.find('(')
                                    .and_then(|i| keyvalue[i + 1..].find(')')
                                        .map(|j| &keyvalue[i + 1..i + 1 + j]))
                                    .and_then(|s| s.parse::<usize>().ok())
                                    .unwrap_or(0); // fallback if parsing fails
                                if let Some(inner) = filtered_map_file.get(&key) {


                                    let mn  = self.percentile_nearest_rank_from_btreeptr(&inner,val as f64);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            } 
                            else if(keyvalue.starts_with("percentile_linear("))
                            {   
                                let val: usize = keyvalue.find('(')
                                    .and_then(|i| keyvalue[i + 1..].find(')')
                                        .map(|j| &keyvalue[i + 1..i + 1 + j]))
                                    .and_then(|s| s.parse::<usize>().ok())
                                    .unwrap_or(0); // fallback if parsing fails
                                if let Some(inner) = filtered_map_file.get(&key) {

                                    let mn  = self.percentile_linear_from_btreeptr(&inner,val as f64);
                                    let resultstr 
                                                = format!("{}->{}:{}",keyName,keyvalue,mn);
                                    result_lines.push(resultstr);
                                }
                            }
                        }                     
                    }                

                }
            }
        }
        //println!("Got the matching pointers");
        if(returnwhat == 2){
            //only pointers
            (Vec::new(),result_lines)
        }
        else{ //only '1' and '2' values currently supported
            (Vec::new(),result_lines)
        }
    }

    pub fn get_fromkey_onlypointersForDeleted(
        &self,
        file_name: &str,
        key_map: &IndexMap<String, String>,
    ) -> Option<Vec<usize>> {
        // Step 1: Gather all pointer lists for each keyname-value pair
        let mut sets: Vec<HashSet<usize>> = Vec::new();

        for (keyname, keyvalue) in key_map {
            let ptrs: HashSet<usize> = self
                .file_key_pointer_fordeleted_map
                .get(file_name)?      // -> &HashMap<_, Vec<usize>>
                .get(keyname)?        // -> &Vec<usize>
                .get(&NumKey(keyvalue.into()) )?       // -> &Vec<usize>
                .iter()               // -> Iterator<Item=&usize>
                .copied()             // -> Iterator<Item=usize>
                .collect();           // -> HashSet<usize>

            sets.push(ptrs);
        }

        // Step 2: Intersect all sets
        let common_ptrs = match sets.split_first() {
            Some((first, rest)) => {
                rest.iter().fold(first.clone(), |acc, s| acc.intersection(s).copied().collect())
            }
            None => return None,
        };

        if common_ptrs.is_empty() {
            None
        } else {
            Some(common_ptrs.into_iter().collect())
        }
    }

    pub fn get_fromkey_onlypointers(
        &self,
        file_name: &str,
        key_map: &IndexMap<String, String>,
    ) -> Option<Vec<usize>> {
        // Step 1: Gather all pointer lists for each keyname-value pair
        let mut sets: Vec<HashSet<usize>> = Vec::new();

        for (keyname, keyvalue) in key_map {
            let ptrs: HashSet<usize> = self
                .file_key_pointer_map
                .get(file_name)?      // -> &HashMap<_, Vec<usize>>
                .get(keyname)?        // -> &Vec<usize>
                .get(&NumKey(keyvalue.into()) )?       // -> &Vec<usize>
                .iter()               // -> Iterator<Item=&usize>
                .copied()             // -> Iterator<Item=usize>
                .collect();           // -> HashSet<usize>

            sets.push(ptrs);
        }

        // Step 2: Intersect all sets
        let common_ptrs = match sets.split_first() {
            Some((first, rest)) => {
                rest.iter().fold(first.clone(), |acc, s| acc.intersection(s).copied().collect())
            }
            None => return None,
        };

        if common_ptrs.is_empty() {
            None
        } else {
            Some(common_ptrs.into_iter().collect())
        }
    }

    pub fn extract_prim_keyname_value_map(&self, json: &Value) -> std::io::Result<IndexMap<String, String>> {
        let mut map = IndexMap::new();

        let key_field = json.get("primkey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing or invalid 'primkey' field",
            ))?;

        for keyname in key_field.split(',').map(str::trim) {
            if keyname.is_empty() {
                continue;
            }

            let value_opt = json.get(keyname).or_else(|| self.find_key_recursively(json, keyname));

            if let Some(value) = value_opt {
                let value_str = match value {
                    Value::String(s) => s.clone(),
                    _ => value.to_string().trim_matches('"').to_string(),
                };
                //do reverse replacement
                let mut key_t = keyname.to_string();
                if(key_t.contains(self.settings.indexnamevaluedelimiter)){
                    key_t = key_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                }
                if(key_t.contains(self.settings.indexdelimiter)){
                    key_t = key_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                }
                if(key_t.contains(self.settings.recorddelimiter)){
                    key_t = key_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                }
                let mut value_str_t = value_str.to_string();
                if(value_str_t.contains(self.settings.indexnamevaluedelimiter)){
                    value_str_t = value_str_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                }
                if(value_str_t.contains(self.settings.indexdelimiter)){
                    value_str_t = value_str_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                }
                if(value_str_t.contains(self.settings.recorddelimiter)){
                    value_str_t = value_str_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                }
                map.insert(key_t, value_str_t);
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Key '{}' not found in JSON", keyname),
                ));
            }
        }

        Ok(map)
    }    

    pub fn extract_keyname_value_map(&self, json: &Value) -> std::io::Result<IndexMap<String, String>> {
        let mut map = IndexMap::new();

        let key_field = json.get("key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing or invalid 'key' field",
            ))?;

        for keyname in key_field.split(',').map(str::trim) {
            if keyname.is_empty() {
                continue;
            }

            let value_opt = json.get(keyname).or_else(|| self.find_key_recursively(json, keyname));

            if let Some(value) = value_opt {
                let value_str = match value {
                    Value::String(s) => s.clone(),
                    _ => value.to_string().trim_matches('"').to_string(),
                };
                //do reverse replacement
                let mut key_t = keyname.to_string();
                if(key_t.contains(self.settings.indexnamevaluedelimiter)){
                    key_t = key_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                }
                if(key_t.contains(self.settings.indexdelimiter)){
                    key_t = key_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                }
                if(key_t.contains(self.settings.recorddelimiter)){
                    key_t = key_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                }
                let mut value_str_t = value_str.to_string();
                if(value_str_t.contains(self.settings.indexnamevaluedelimiter)){
                    value_str_t = value_str_t.replace(self.settings.indexnamevaluedelimiter,&self.settings.repindexnamevaluedelimiter.to_string());
                }
                if(value_str_t.contains(self.settings.indexdelimiter)){
                    value_str_t = value_str_t.replace(self.settings.indexdelimiter,&self.settings.repindexdelimiter.to_string());
                }
                if(value_str_t.contains(self.settings.recorddelimiter)){
                    value_str_t = value_str_t.replace(self.settings.recorddelimiter,&self.settings.reprecorddelimiter.to_string());
                }
                map.insert(key_t, value_str_t);
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Key '{}' not found in JSON", keyname),
                ));
            }
        }

        Ok(map)
    }

    pub fn log_error_message(&mut self, message: &str) -> std::io::Result<()> {
        writeln!(self.errorlog_file, "{}", message)?;
        self.errorlog_file.flush()?;

        Ok(())
    }

    pub fn log_message(&mut self, message: &str) -> std::io::Result<()> {
        if(self.settings.debugmode){
            println!("message: {}", &message.chars().take(self.settings.maxlogtoconsolelength).collect::<String>());
        }
        writeln!(self.log_file, "{}", message)?;
        self.log_file.flush()?;
        self.log_line_count += 1;

        // Check if rollover needed
        // Check if rollover is needed
        if self.log_line_count >= self.settings.logmaxlines {
            // Create timestamped backup file name
            let timestamp = Self::current_timestamp_forlog();
            let base_log_path = format!("{}/jblox.log", self.logdir);
            let backup_log_path = format!("{}/jblox_{}.log", self.logdir, timestamp);

            // Close the current file and rename it
            drop(&self.log_file); // Flush & close
            std::fs::rename(&base_log_path, &backup_log_path)?;

            // Open a new file with original name (overwrites if already exists)
            let new_log_file = BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&base_log_path)?
            );
            self.log_file = new_log_file;
            self.log_line_count = 0;
        }
        Ok(())
    }
        
    pub fn to_compact_json(&self, json: &Value) -> io::Result<String> {
        serde_json::to_string(json).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("JSON serialization failed: {}", e))
        })
    }

    pub fn compact_json_str(&self, json_str: &str) -> io::Result<String> {
        let value: Value = serde_json::from_str(json_str).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid JSON: {}", e))
        })?;
        
        serde_json::to_string(&value).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Serialization failed: {}", e))
        })
    }
    fn current_timestamp(&self) -> String {
        let now = chrono::Local::now();
        now.format("%d%m%y%H%M%S%6f").to_string()
    }
    
    fn current_timestamp_forlog() -> String {
        Local::now().format("%d-%m-%y-%H-%M-%S-%6f").to_string()
    } 


    pub fn handle_request(&mut self, input: &str) -> std::io::Result<Vec<String>> {

        let parsed: Value = serde_json::from_str(input).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Invalid JSON: {e}"))
        })?;

        let op = parsed.get("op").and_then(Value::as_str)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing or invalid 'op'"))?;

        let data = parsed.get("data")
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing 'data'"))?;

        let data_str = serde_json::to_string(data).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Failed to serialize 'data': {e}"))
        })?;

        let ok_response = || Ok(vec!["ok".to_string()]);

        match op {
            "insertduplicate" => {
                self.insertduplicate(&data_str)?;
                ok_response()
            }
            "insert" => {
                self.insert(&data_str)?;
                ok_response()
            }
            "updateall" => {
                self.updateall(&data_str)?;
                ok_response()
            }
            "update" => {
                self.update(&data_str)?;
                ok_response()
            }
            "deleteall" => {
                self.deleteall(&data_str)?;
                ok_response()
            }
            "delete" => {
                self.delete(&data_str)?;
                ok_response()
            }
            "undo" => {
                self.undo(&data_str)?;
                ok_response()
            }
            "gethist" => self.gethist(&data_str),
            "gethistdesc" => self.gethistdesc(&data_str),
            "gethistall" => self.gethistall(&data_str),
            "gethistalldesc" => self.gethistalldesc(&data_str),
            "getall" => self.getall(&data_str),
            "get" => self.get(&data_str),
            "getalldesc" => self.getalldesc(&data_str),
            "getdesc" => self.getdesc(&data_str),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Unknown op: {op}"))),
        }
    }

/////////////////////////////////////////////////////
///------------- recovery module --------------------

pub fn sanitycheck(& mut self) -> io::Result<()> {
    self.recover(self.settings.sanitycheckgobacksec, true);
    Ok(())
}
 
    /// Scan jblox.log for the last `seconds_back` seconds and complete any
    /// in-flight update requests by finishing missing D/I steps.
    /// use_intersection will tell if its actually update or updateall
    /// use_intersection -> update; else updateall
pub fn recover(
    &mut self,
    seconds_back: usize,
    use_intersection: bool,
) -> std::io::Result<()> {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::fs::File;
    use std::io::{self, BufRead, BufReader};
    use chrono::{DateTime, Local, NaiveDateTime, TimeDelta, Utc};
    use serde_json::{json, Value};
    use chrono::Duration as ChronoDuration; 

    let mut processfulllogfile = false;

    //let log_path = format!("{}/jblox_recovery_test.log", self.logdir);
    let log_path = format!("{}/jblox.log", self.logdir);
    println!("log_path: {}", log_path);

    // Pass-1 storage (no meta parsing here)
    let mut ts_lines: BTreeMap<String, Vec<String>> = BTreeMap::new(); // ONLY 0/1 lines
    let mut ts_11_raw_update: BTreeMap<String, String> = BTreeMap::new();     // latest raw '11-update' per ts
    let mut ts_11_raw_updateall: BTreeMap<String, String> = BTreeMap::new();     // latest raw '11-update' per ts
    let mut ts_11_raw_delete: BTreeMap<String, String> = BTreeMap::new();     // latest raw '11-update' per ts
    let mut ts_11_raw_deleteall: BTreeMap<String, String> = BTreeMap::new();     // latest raw '11-update' per ts
    let mut ts_11_raw_undo: BTreeMap<String, String> = BTreeMap::new();     // latest raw '11-update' per ts
    let mut ts_11_raw_insert: BTreeMap<String, String> = BTreeMap::new();     // latest raw '11-update' per ts
    let mut ts_11_raw_insertduplicate: BTreeMap<String, String> = BTreeMap::new();     // latest raw '11-update' per ts

    let mut ts_11_raw_all: BTreeSet<String> = BTreeSet::new();
    let mut R_11_raw_all: BTreeSet<String> = BTreeSet::new();

    // inline ts parser function used only to decide when to start (first 11 after cutoff)
    let mut parse_ts_to_utc 
                = |ts: &str| -> Option<chrono::DateTime<chrono::Utc>> {
        use chrono::{DateTime, NaiveDateTime, Utc};

        // RFC 3339
        if let Ok(dt) = DateTime::parse_from_rfc3339(ts) {
            return Some(dt.with_timezone(&Utc));
        }

        // 18-digit compact: yyMMddHHmmssffffff (e.g., 230825050212263942)
        if ts.len() == 18 && ts.as_bytes().iter().all(|b| b.is_ascii_digit()) {
            if let Ok(ndt) = NaiveDateTime::parse_from_str(ts, "%d%m%y%H%M%S%f") {
                return Some(DateTime::<Utc>::from_utc(ndt, Utc));
            }
        }

        // pure digits → epoch secs/millis
        if !ts.is_empty() && ts.as_bytes().iter().all(|b| b.is_ascii_digit()) {
            if let Ok(n) = ts.parse::<i64>() {
                if ts.len() >= 13 {
                    let secs  = n / 1_000;
                    let nanos = ((n % 1_000) as u32) * 1_000_000;
                    if let Some(dt) = DateTime::<Utc>::from_timestamp(secs, nanos) { return Some(dt); }
                } else {
                    if let Some(dt) = DateTime::<Utc>::from_timestamp(n, 0) { return Some(dt); }
                }
            }
        }

        // other formats you already had
        if let Ok(ndt) = NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S%.f") {
            return Some(DateTime::<Utc>::from_utc(ndt, Utc));
        }
        if let Ok(ndt) = NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S%.f") {
            return Some(DateTime::<Utc>::from_utc(ndt, Utc));
        }
        if let Ok(ndt) = NaiveDateTime::parse_from_str(ts, "%Y%m%d%H%M%S") {
            return Some(DateTime::<Utc>::from_utc(ndt, Utc));
        }

        None
    };
    ///// parser function till here //////////

    // ===================== PASS 1: top→bottom =====================
    let file = File::open(&log_path)?;
    let mut rdr  = BufReader::new(file);
    let mut cutoff_utc: DateTime<Utc> = Utc::now();;

    if(seconds_back != 0){
        let mut file_eof = std::fs::File::open(&log_path)?;
        let mut pos = file_eof.seek(SeekFrom::End(0))?;
        // keep trying previous lines until parse succeeds or BOF
        let mut ndt_opt: Option<DateTime<Utc>> = None;

        loop {
            // collect the previous line (walking backward from current `pos`)
            let mut buf = [0u8; 1];
            let mut line = Vec::new();

            // walk backward to previous '\n' (or BOF), accumulating bytes
            while pos > 0 {
                pos -= 1;
                file_eof.seek(SeekFrom::Start(pos))?;
                file_eof.read_exact(&mut buf)?;
                if buf[0] == b'\n' {
                    if !line.is_empty() {
                        break; // reached start of a line we collected
                    } else {
                        continue; // skip consecutive newlines at EOF
                    }
                } else {
                    line.push(buf[0]);
                }
            }

            // if we’re at EOF and didn’t collect anything, file is empty / no more lines
            if pos == 0 && line.is_empty() {
                break;
            }

            // turn bytes into the line string
            line.reverse();
            let line_str = String::from_utf8_lossy(&line);
            let ts_str = line_str.splitn(2, self.settings.indexnamevaluedelimiter).next().unwrap_or("");

            if let Some(ts) = parse_ts_to_utc(ts_str) {
                ndt_opt = Some(ts);
                break;
            }

            // if we were already at BOF, we’re done
            if pos == 0 {
                break;
            }
        }

        // act on the result
        let ndt = ndt_opt.ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "failed to parse a valid timestamp from any line (reached start of file)",
            )
        })?;

        // this works in chrono 0.4
        cutoff_utc = ndt - ChronoDuration::seconds(seconds_back as i64);
        println!("cutoff_utc for sanity check: {}", cutoff_utc);
    }
    else{
        processfulllogfile = true;
    }

    // reset buffer to start of file
    rdr.seek(SeekFrom::Start(0))?;

    let mut started = false;

    for lr in rdr.lines() {
        let line = lr?;
        let mut it = line.splitn(4, self.settings.indexnamevaluedelimiter);
        let ts   = match it.next() { Some(x) => x, None => continue };
        let code = it.next().unwrap_or("");
        let op   = it.next().unwrap_or("");
        let rest = it.next().unwrap_or("");

        if !started {
            if code == "11" {
                if !processfulllogfile {
                    if let Some(dt) = parse_ts_to_utc(ts) {
                        if dt < cutoff_utc { 
                            continue; 
                        }
                    }
                }
                started = true;
                // on '11' → ONLY remember latest raw 11, DO NOT touch ts_lines
                match  op{
                     "update" => {
                        ts_11_raw_update.insert(ts.to_string(), line.clone());
                     }
                     "delete" => {
                        ts_11_raw_delete.insert(ts.to_string(), line.clone());
                     }
                     "updateall" => {
                        ts_11_raw_updateall.insert(ts.to_string(), line.clone());
                     }
                     "deleteall" => {
                        ts_11_raw_deleteall.insert(ts.to_string(), line.clone());
                     }
                     "undo" => {
                        ts_11_raw_undo.insert(ts.to_string(), line.clone());
                     }
                     "insert" => {
                        ts_11_raw_insert.insert(ts.to_string(), line.clone());
                     }
                     "insertduplicate" => {
                        ts_11_raw_insertduplicate.insert(ts.to_string(), line.clone());
                     }                     
                     _ => {}
                }
            }
            continue;
        }

        match code {
            // '00-Done' → drop this ts from both maps
            "00" => {
                let op_is_done   = op.eq_ignore_ascii_case("Done");
                let rest_is_done = rest.eq_ignore_ascii_case("Done");
                if op_is_done || rest_is_done {
                    // Remove regardless of which map currently has the key.
                    //ts_11_raw_update.remove(ts);
                    //ts_11_raw_delete.remove(ts);
                    //ts_11_raw_updateall.remove(ts);
                    //ts_11_raw_deleteall.remove(ts);
                    //ts_11_raw_undo.remove(ts);
                    //ts_11_raw_insert.remove(ts);
                    //ts_11_raw_insertduplicate.remove(ts);
                    //let _ = ts_lines.remove(ts);
                    // continue; // (optional) make intent explicit
                }
            }

            // '11-update' → refresh latest raw 11 ONLY (no ts_lines entry)
            "11" =>{
                if(!R_11_raw_all.contains(ts)){
                    match  op{
                        "update" => {
                            ts_11_raw_update.insert(ts.to_string(), line.clone());
                        }
                        "delete" => {
                            ts_11_raw_delete.insert(ts.to_string(), line.clone());
                        }
                        "updateall" => {
                            ts_11_raw_updateall.insert(ts.to_string(), line.clone());
                        }
                        "deleteall" => {
                            ts_11_raw_deleteall.insert(ts.to_string(), line.clone());
                        }
                        "undo" => {
                            ts_11_raw_undo.insert(ts.to_string(), line.clone());
                        }
                        "insert" => {
                            ts_11_raw_insert.insert(ts.to_string(), line.clone());
                        }
                        "insertduplicate" => {
                            ts_11_raw_insertduplicate.insert(ts.to_string(), line.clone());
                        }
                        _ => {}
                    }
                }
            }

            // '1' / '0' → keep only if we've seen an '11' for this ts
            "1" | "0" => {
                //insert only if it doesn't exist in R_11_raw_all
                if(!R_11_raw_all.contains(ts)){
                    if (ts_11_raw_update.contains_key(ts) 
                        ||
                        ts_11_raw_delete.contains_key(ts) 
                        ||
                        ts_11_raw_updateall.contains_key(ts) 
                        ||
                        ts_11_raw_deleteall.contains_key(ts) 
                        ||
                        ts_11_raw_undo.contains_key(ts) 
                        ||
                        ts_11_raw_insert.contains_key(ts)
                        ||
                        ts_11_raw_insertduplicate.contains_key(ts)){
                        ts_lines.entry(ts.to_string()).or_default().push(line);
                    }
                }

            }
            "R" =>{
                R_11_raw_all.insert(ts.to_string());
            }

            _ => {}
        }
    }

    //create sorted BTreeSet of all the timestamps, as all recovery steps need 
    //to be done in the order of ascending timestamp
    
    ts_11_raw_all.extend(ts_11_raw_update.keys().cloned());
    ts_11_raw_all.extend(ts_11_raw_updateall.keys().cloned());
    ts_11_raw_all.extend(ts_11_raw_delete.keys().cloned());
    ts_11_raw_all.extend(ts_11_raw_deleteall.keys().cloned());
    ts_11_raw_all.extend(ts_11_raw_undo.keys().cloned());
    ts_11_raw_all.extend(ts_11_raw_insert.keys().cloned());
    ts_11_raw_all.extend(ts_11_raw_insertduplicate.keys().cloned());

    // ===================== Build ReqState from kept 0/1 lines =====================
    #[derive(Default, Debug)]
    struct ReqState {
        op: String, //update;delete;insert;undo
        expect_d: HashSet<usize>,
        expect_i: HashSet<usize>,
        done_d:   HashSet<usize>,
        done_i:   HashSet<usize>,
        count_i:   usize,
		count_d:   usize,        
    }

    for tsu in &ts_11_raw_all {
        ///////////////////////////// update and updateall ///////////////////
        if let Some(raw11) = ts_11_raw_update.get(tsu).or(ts_11_raw_updateall.get(tsu)) {
            let mut reqs: HashMap<String, ReqState> = HashMap::new();
            let mut linesfound = false;

            let mut it = raw11.splitn(4, self.settings.indexnamevaluedelimiter);
            let _ts   = it.next().unwrap_or("");
            let code  = it.next().unwrap_or("");
            let op    = it.next().unwrap_or("");
            let rest  = it.next().unwrap_or("");
            let json: serde_json::Value = serde_json::from_str(rest)?;

            let file_name: String = json["keyobj"]
                .as_str()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Missing 'keyobj'"))?
                .to_string();


            // now borrow immutably; multiple & borrows may overlap
            let empty: BTreeMap<usize, usize> = BTreeMap::new();
            let key_lines: &BTreeMap<usize, usize> =
                self.file_key_line_map.get(&file_name).unwrap_or(&empty);

            if let Some(lines_vec) = ts_lines.get(tsu) {
                linesfound = true;
                let rs = reqs.entry(tsu.clone()).or_default();
                //230825071853155767-1-1728-T-D-I
                //230825071853155767-0-1728-D
                //230825071853155767-0-2326-I
                for line in lines_vec {
                    let mut it = line.splitn(4, self.settings.indexnamevaluedelimiter);
                    let _ts  = it.next().unwrap_or("");
                    let code = it.next().unwrap_or("");
                    let op   = it.next().unwrap_or("");
                    let rest = it.next().unwrap_or("");

                    match code {
                        "1" => {
                            let is_digits = !op.is_empty() && op.as_bytes().iter().all(|b| b.is_ascii_digit());
                            if is_digits {
                                if let Ok(base_ptr) = op.parse::<usize>() {
                                    let mut need_d = false;
                                    let mut need_i = false;
                                    let mut op_count = 0;
                                    for t in rest.split(self.settings.indexnamevaluedelimiter) {
                                        match t {
                                            "D" => need_d = true,
                                            "I" => need_i = true,
                                            "T" => {},
                                            other => 
                                                    {
                                                        op_count = other.parse::<usize>().unwrap_or(0);
                                                    }
                                        }
                                    }
                                    let mut recorddeleted = false;
                                    if need_d { 
                                        rs.expect_d.insert(base_ptr); 
                                        rs.count_d = op_count;

                                        //check if record was deleted
                                        //check if base_ptr points to a deleted record
                                        //check if record was deleted at the backend
                                        //before marking it done
                                        if key_lines
                                            .get(&base_ptr)
                                            .map_or(false, |&len| len >= ADD_TOMARK_REC_DELETE_IN_RECORDKEY)
                                        {
                                            rs.done_d.insert(base_ptr);
                                            recorddeleted = true;
                                        }
                                    }
                                    if need_i { 
                                        rs.expect_i.insert(base_ptr); 
                                        rs.count_i = op_count;

                                        //check if record was inserted
                                        //check if record corresponding to num was inserted
                                        //need to check only recorddeleted = true, as if record is not
                                        //deleted, no way it was instered as records are deleted first
                                        let mut insertrecordfound = false;
                                        if(recorddeleted){
                                            match self.print_line_forpointer_uselen(&file_name, base_ptr, true) {
                                                Ok(line) => {
                                                    let restorejson = extract_json(&line,self.settings.recorddelimiter);
                                                    let mut restoreprimkey_map: IndexMap<String, String> = self.extract_prim_keyname_value_map(&restorejson)?;
                                                    //update recid with the timestamp as for undo records are inserted with new timestamp
                                                    restoreprimkey_map.insert("recid".to_string(), _ts.to_string());
                                                    if let Some(restoreptrs) = self.get_fromkey_onlypointers(&file_name, &restoreprimkey_map) {
                                                        if(restoreptrs.len() > 0){
                                                            //corresponding record was inserted
                                                            rs.done_i.insert(base_ptr);
                                                            insertrecordfound = true;
                                                        }
                                                    }
                                                    //check again in deleted records
                                                    if(!insertrecordfound){
                                                        if let Some(restoreptrs) = self.get_fromkey_onlypointersForDeleted(&file_name, &restoreprimkey_map) {
                                                            if(restoreptrs.len() > 0){
                                                                //corresponding record was inserted
                                                                rs.done_i.insert(base_ptr);
                                                                insertrecordfound = true;
                                                            }
                                                        }                                                                    
                                                    }                                                    
                                                        
                                                }
                                                Err(e) => {
                                                    //record not found
                                                    //do nothing
                                                }
                                    
                                            } 
                                        }                                       
                                    }
                                }
                            }
                        }
                        "0" => {
                            //no need to do anything as records are validated in "1"
                        }
                        _ => {}
                    }
                }
                //total count of operations will be mentioned in the last part
                //so need to make sure that all pts are mentioned by checking
                //if pts mentioned matches the count, as its possible that log
                //got interrupted. This will handle situations wherein 4 records
                //were to be updated, but in log only 3 'T' records are mentioned
                if(rs.count_i != rs.expect_i.len() || rs.count_d != rs.expect_d.len()){
                    linesfound = false;
                }           
                   
            }

            
            if(linesfound){
                if let Some(rs) = reqs.get_mut(tsu) {
                    rs.op = op.to_string();

                    // ---- 1) Build pointer vectors (pending work) ----
                    let mut del_ptrs: Vec<usize> =
                        rs.expect_d.difference(&rs.done_d).copied().collect();
                    let mut ins_ptrs: Vec<usize> =
                        rs.expect_i.difference(&rs.done_i).copied().collect();

                    // (optional but good for determinism), not sure if needed
                    //del_ptrs.sort_unstable();
                    //del_ptrs.dedup();
                    //ins_ptrs.sort_unstable();
                    //ins_ptrs.dedup();

                    if(!(del_ptrs.is_empty() && ins_ptrs.is_empty())){
                        // mirror your intention logs
                        self.log_message(&format!("{}-R-update(all)-{}", tsu, rest.clone()))?;
                    }
                    
                    // ---- 2a) Process pending deletes ----
                    if !del_ptrs.is_empty() {

                        for p in &del_ptrs {
                            self.log_message(&format!("{}-1-{}-T-D", tsu, p))?;
                        }
                        // idempotent batch delete
                        self.delete_using_pointerVector(&file_name, del_ptrs.clone(), "u".to_string(), &tsu);
                    }

                    // ---- 2b) Process pending inserts ----
                    if !ins_ptrs.is_empty() {

                        for p in &ins_ptrs {
                            self.log_message(&format!("{}-1-{}-T-I", tsu, p))?;
                        }



                        for &base_ptr in &ins_ptrs {
                            let origrec  = self.print_line_forpointer_uselen(&file_name, base_ptr, true)?;
                            let origjson = extract_json(&origrec, self.settings.recorddelimiter);
                            let modjson  = json.clone();
                            // primkey → merge → dedupe → insert
                            let mut key_map 
                                        = self.extract_prim_keyname_value_map(&origjson)?;
                            key_map.entry("primkey".to_string()).or_insert("".to_string());   



                            let key_set: HashSet<&str> = key_map.keys().map(|s| s.as_str()).collect();

                            if !origjson.is_null() {
                                let mergedjson = merge(&origjson,&modjson,  &key_set);
                                self.insert_duplicate_frmObject(&mergedjson, &tsu);

                            }
                        }
                        // final marker 
                        let _ = self.log_message(&format!("{tsu}-00-Done"));                          
                    }
          

                }   
            }
            else{
                //complete operation need to be redone
                let replaced = raw11.splitn(3, self.settings.indexnamevaluedelimiter).collect::<Vec<_>>();
                let new_raw = format!("{}-R-{}", replaced[0], replaced[2]);
                let startmsg =format!("{}", new_raw);
                self.log_message(&startmsg)?;
                let jsonstr = raw11.splitn(4, self.settings.indexnamevaluedelimiter).nth(3).unwrap_or("");
                if(ts_11_raw_update.contains_key(tsu)){
                    self.updateWTS(jsonstr,tsu.clone());            
                }
                else{
                    self.updateallWTS(jsonstr,tsu.clone());            

                }

            }     
        }
        ///////////////////////////// delete and deleteall ///////////////////
        else if let Some(raw11) = ts_11_raw_delete.get(tsu).or(ts_11_raw_deleteall.get(tsu)) {
            let mut reqs: HashMap<String, ReqState> = HashMap::new();
            let mut linesfound = false;

            let mut it = raw11.splitn(4, self.settings.indexnamevaluedelimiter);
            let _ts   = it.next().unwrap_or("");
            let code  = it.next().unwrap_or("");
            let op    = it.next().unwrap_or("");
            let rest  = it.next().unwrap_or("");
            let json: serde_json::Value = serde_json::from_str(rest)?;

            let file_name: String = json["keyobj"]
                .as_str()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Missing 'keyobj'"))?
                .to_string();

            let empty: BTreeMap<usize, usize> = BTreeMap::new();
            let key_lines: &BTreeMap<usize, usize> =
                self.file_key_line_map.get(&file_name).unwrap_or(&empty);

            if let Some(lines_vec) = ts_lines.get(tsu) {
                linesfound = true;
                let rs = reqs.entry(tsu.clone()).or_default();
                //230825072001690221-1-132696-T-D
                //230825072001690221-0-131450-D
                for line in lines_vec {
                    let mut it = line.splitn(4, self.settings.indexnamevaluedelimiter);
                    let _ts  = it.next().unwrap_or("");
                    let code = it.next().unwrap_or("");
                    let op   = it.next().unwrap_or("");
                    let rest = it.next().unwrap_or("");
                    match code {
                        "1" => {
                            let is_digits = !op.is_empty() && op.as_bytes().iter().all(|b| b.is_ascii_digit());
                            if is_digits {
                                if let Ok(base_ptr) = op.parse::<usize>() {
                                    let mut need_d = false;
                                    let mut op_count = 0;
                                    for t in rest.split(self.settings.indexnamevaluedelimiter) {
                                        match t {
                                            "D" => need_d = true,
                                            "T" => {},
                                            other => 
                                                    {
                                                        op_count = other.parse::<usize>().unwrap_or(0);
                                                    }
                                        }
                                    }

                                    if need_d { rs.expect_d.insert(base_ptr); rs.count_d = op_count}
                                }
                            }
                        }
                        "0" => {
                            let is_digits = !op.is_empty() && op.as_bytes().iter().all(|b| b.is_ascii_digit());
                            if is_digits {
                                if let Ok(p) = op.parse::<usize>() {
                                    match rest {
                                        "D" => { 
                                            //check if record was deleted at the backend
                                            //before marking it done
                                            if key_lines
                                                .get(&p)
                                                .map_or(false, |&len| len >= ADD_TOMARK_REC_DELETE_IN_RECORDKEY)
                                            {
                                                rs.done_d.insert(p);
                                            }  
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                //total count of operations will be mentioned in the last part
                //so need to make sure that all pts are mentioned by checking
                //if pts mentioned matches the count, as its possible that log
                //go interrupted. This will handle situations wherein 4 records
                //were to be deleted, but in log only 3 'T' records are mentioned
                if(rs.count_d != rs.expect_d.len()){
                    linesfound = false;
                }           
                   
            }

            
            if(linesfound){
                if let Some(rs) = reqs.get_mut(tsu) {

                    rs.op = op.to_string();

                    // ---- 1) Build pointer vectors (pending work) ----
                    let mut del_ptrs: Vec<usize> =
                        rs.expect_d.difference(&rs.done_d).copied().collect();

                    // ---- 2a) Process pending deletes ----
                    if !del_ptrs.is_empty() {
                        // mirror your intention logs
                        self.log_message(&format!("{}-R-delete(all)-{}", tsu, rest.clone()))?;

                        for p in &del_ptrs {
                            self.log_message(&format!("{}-1-{}-T-D", tsu, p))?;
                        }
                        // idempotent batch delete
                        self.delete_using_pointerVector(&file_name, del_ptrs.clone(), "d".to_string(), &tsu);
                        // final marker 
                        let _ = self.log_message(&format!("{tsu}-00-Done"));            
                    }


                }   
            }
            else{
                //complete operation need to be redone
                let replaced = raw11.splitn(3, self.settings.indexnamevaluedelimiter).collect::<Vec<_>>();
                let new_raw = format!("{}-R-{}", replaced[0], replaced[2]);
                let startmsg =format!("{}", new_raw);
                self.log_message(&startmsg)?;
                let jsonstr = raw11.splitn(4, self.settings.indexnamevaluedelimiter).nth(3).unwrap_or("");

                if(ts_11_raw_delete.contains_key(tsu)){
                    self.deleteWTS(jsonstr,tsu.clone());            
                }
                else{
                    self.deleteallWTS(jsonstr,tsu.clone());            

                }     

            }     
        }//////////////////////////////////// insert and insertduplicate //////////////////////////////////        
        if let Some(raw11) 
                = ts_11_raw_insert.get(tsu).or(ts_11_raw_insertduplicate.get(tsu)) {
            let mut reqs: HashMap<String, ReqState> = HashMap::new();
            let mut reqs: HashMap<String, ReqState> = HashMap::new();
            let mut linesfound = false;

            let mut it = raw11.splitn(4, self.settings.indexnamevaluedelimiter);
            let _ts   = it.next().unwrap_or("");
            let code  = it.next().unwrap_or("");
            let op    = it.next().unwrap_or("");
            let rest  = it.next().unwrap_or("");
            let json: serde_json::Value = serde_json::from_str(rest)?;

            let file_name: String = json["keyobj"]
                .as_str()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Missing 'keyobj'"))?
                .to_string();

            if let Some(lines_vec) = ts_lines.get(tsu) {
                let rs = reqs.entry(tsu.clone()).or_default();
                //230825071853155767-0-2326-I
                for line in lines_vec {
                    let mut it = line.splitn(4, self.settings.indexnamevaluedelimiter);
                    let _ts  = it.next().unwrap_or("");
                    let code = it.next().unwrap_or("");
                    let op   = it.next().unwrap_or("");
                    let rest = it.next().unwrap_or("");                    
                    match code {
                        "0" => {
                            let is_digits = !op.is_empty() && op.as_bytes().iter().all(|b| b.is_ascii_digit());
                            if is_digits {
                                if let Ok(p) = op.parse::<usize>() {
                                    match rest {
                                        "I" => { 
                                            //'0' will indicate that insert was successfully done
                                            //but '00' done was not found, so just log that
                                            //Note that only 1 record at a time is inserted
                                            //so any '0' would indicate that last insert was 
                                            //successful

                                            //but do make sure that record exist before doing so
                                            let exists: bool = self.file_key_line_map
                                                .get(&file_name)                   // Option<&BTreeMap<usize, usize>>
                                                .map_or(false, |m| m.contains_key(&p));  // false if None                                           

                                            if(exists)
                                            {
                                                linesfound = true; 

                                            }                                             

                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                   
            }
            if(!linesfound){

                if(ts_11_raw_insertduplicate.contains_key(tsu)){

                    let json_str = raw11.splitn(4, self.settings.indexnamevaluedelimiter).nth(3).unwrap_or("");
                    self.log_message(&format!("{}-R-insertduplicate-{}",tsu, json_str))?;

                    // Do not check for duplicates
                    let result =  self.insert_main(json_str, false, &tsu);
                    match &result {
                        Ok(vec) => {}
                            //println!("{}-{:?}",tsu, vec);
                        //}
                        Err(e) => {
                            self.log_message(&format!("{}-{}",tsu, e));
                        }
                    }                      
                    self.log_message(&format!("{}-00-Done", tsu));    
         
                }
                else{
                    let json_str = raw11.splitn(4, self.settings.indexnamevaluedelimiter).nth(3).unwrap_or("");
                    self.log_message(&format!("{}-R-insert-{}",tsu, json_str))?;

                    // Check for duplicates
                    let result =  self.insert_main(json_str, true, &tsu);
                    match &result {
                        Ok(vec) => {}
                            //println!("{}-{:?}", tsu, vec);
                        //}
                        Err(e) => {
                            self.log_message(&format!("{}-{}",tsu, e));
                        }
                    }                      
                    self.log_message(&format!("{}-00-Done", tsu));             

                }

          
  
            }
     
        }
        ///////////////////////////// undo ///////////////////
        if let Some(raw11) = ts_11_raw_undo.get(tsu) {
            let mut reqs: HashMap<String, ReqState> = HashMap::new();
            let mut linesfound = false;
            let mut it = raw11.splitn(4, self.settings.indexnamevaluedelimiter);
            let _ts   = it.next().unwrap_or("");
            let code  = it.next().unwrap_or("");
            let op    = it.next().unwrap_or("");
            let rest  = it.next().unwrap_or("");
            let json: serde_json::Value = serde_json::from_str(rest)?;

            let file_name: String = json["keyobj"]
                .as_str()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Missing 'keyobj'"))?
                .to_string();

            let empty: BTreeMap<usize, usize> = BTreeMap::new();

            let key_lines: &BTreeMap<usize, usize> =
                self.file_key_line_map.get(&file_name).unwrap_or(&empty); 
            if let Some(lines_vec) = ts_lines.get(tsu) {
                let rs = reqs.entry(tsu.clone()).or_default();
                //230825050212263942-11-undo-{...}
                //230825050212263942-1-452-D-6014-I or 2308-1-6014-I
                //230825050212263942-0-452839462-D
                //230825050212263942-0-3435-I
                //230825050212263942-00-Done
                for line in lines_vec {
                    let mut it = line.splitn(3, self.settings.indexnamevaluedelimiter);
                    let _ts  = it.next().unwrap_or("");
                    let code = it.next().unwrap_or("");
                    let rest = it.next().unwrap_or("");
                    let mut needonlyinsert = false;
                    if rest.contains('T') { //'T' will be mentioned only for delete undo
                        needonlyinsert = true;
                    }                      
                    match code {

                        "1" => {
                            let mut need_d = false;
                            let mut need_i = false;
                            let mut op_count = 0;
                          
                            for t in rest.split(self.settings.indexnamevaluedelimiter) {
                                match t {
                                    "D" => {//will be taken care by 'other' match  
                                    },
                                    "I" => { //will be taken care by 'other' match                                           
                                    },
                                    "T" => { needonlyinsert = true; 
                                        //undo for deleted record  e.g. 050925064924439408-1-636-T-I
                                    },
                                    other =>{ 
                                                if(need_d || needonlyinsert){

                                                    //delete set, so set insert
                                                    let is_digits = !other.is_empty() && other.as_bytes().iter().all(|other| other.is_ascii_digit());
                                                    if let Ok(num) = other.parse::<usize>() {
                                                        need_i = true;
                                                        rs.expect_i.insert(num);  
                                                        let mut insertrecordfound = false;                                          
                                                        //check if record corresponding to num was inserted
                                                        match self.print_line_forpointer_uselen(&file_name, num, true) {
                                                            Ok(line) => {
                                                               let restorejson = extract_json(&line,self.settings.recorddelimiter);
                                                               let mut restoreprimkey_map: IndexMap<String, String> = self.extract_prim_keyname_value_map(&restorejson)?;
                                                               //update recid with the timestamp as for undo records are inserted with new timestamp
                                                               restoreprimkey_map.insert("recid".to_string(), _ts.to_string());
                                                                if let Some(restoreptrs) = self.get_fromkey_onlypointers(&file_name, &restoreprimkey_map) {
                                                                    if(restoreptrs.len() > 0){
                                                                        //corresponding record was inserted
                                                                        rs.done_i.insert(num);
                                                                        insertrecordfound = true;
                                                                    }
                                                                }
                                                                //check again in deleted records
                                                                if(!insertrecordfound){
                                                                    if let Some(restoreptrs) = self.get_fromkey_onlypointersForDeleted(&file_name, &restoreprimkey_map) {
                                                                        if(restoreptrs.len() > 0){
                                                                            //corresponding record was inserted
                                                                            rs.done_i.insert(num);
                                                                            insertrecordfound = true;
                                                                        }
                                                                    }                                                                    
                                                                }
                                                                
                                                                  
                                                            }
                                                            Err(e) => {
                                                                println!("error : {}",e);
                                                                //record not found
                                                                //do nothing
                                                            }
                                                
                                                        }


                                                    }
                                                    linesfound = true;

                                                }
                                                else{
                                                    let is_digits = !other.is_empty() && other.as_bytes().iter().all(|other| other.is_ascii_digit());
                                                    if let Ok(num) = other.parse::<usize>() {
                                                        need_d = true; 
                                                        rs.expect_d.insert(num);  

                                                        //check if num points to a deleted record
                                                        //check if record was deleted at the backend
                                                        //before marking it done
                                                        if key_lines
                                                            .get(&num)
                                                            .map_or(false, |&len| len >= ADD_TOMARK_REC_DELETE_IN_RECORDKEY)
                                                        {
                                                            rs.done_d.insert(num);
                                                        }
                                                    } 
                                                    linesfound = true;
                                                }
                                            }
                                }

                            }
                        }
                        "0" => {
                                //nothing to do here as record validations are done in "1"
                            }
                        _ => {}
                    }
                }
            }
            
            if(linesfound){

                if let Some(rs) = reqs.get_mut(tsu) {

                    rs.op = op.to_string();

                    // ---- 1) Build pointer vectors (pending work) ----
                    let mut del_ptrs: Vec<usize> =
                        rs.expect_d.difference(&rs.done_d).copied().collect();
                    let mut ins_ptrs: Vec<usize> =
                        rs.expect_i.difference(&rs.done_i).copied().collect();


                    if(!(del_ptrs.is_empty() && ins_ptrs.is_empty())){
                        // mirror your intention logs
                        self.log_message(&format!("{}-R-undo-{}", tsu, rest.clone()))?;
                    }
                    
                    // ---- 2a) Process pending deletes ----
                    if !del_ptrs.is_empty() {

                        for p in &del_ptrs {
                            self.log_message(&format!("{}-1-{}-T-D", tsu, p))?;
                        }
                        // idempotent batch delete
                        self.delete_using_pointerVector(&file_name, del_ptrs.clone(), "d".to_string(), &tsu);
                    }

                    // ---- 2b) Process pending inserts ----
                    if !ins_ptrs.is_empty() {

                        for p in &ins_ptrs {
                            self.log_message(&format!("{}-1-{}-T-I", tsu, p))?;
                        }

                        for &base_ptr in &ins_ptrs {
                            let origrec  = self.print_line_forpointer_uselen(&file_name, base_ptr, true)?;
                            let origjson = extract_json(&origrec, self.settings.recorddelimiter);

                            if !origjson.is_null() {
                                self.insert_duplicate_frmObject(&origjson, &tsu);


                            }
                        }
                        // final marker 
                        let _ = self.log_message(&format!("{tsu}-00-Done"));                          
                    }
          
                }   
            }
            else{
                //complete operation need to be redone
                let replaced = raw11.splitn(3, self.settings.indexnamevaluedelimiter).collect::<Vec<_>>();
                let new_raw = format!("{}-R-{}", replaced[0], replaced[2]);
                let startmsg =format!("{}", new_raw);
                self.log_message(&startmsg)?;
                let jsonstr = raw11.splitn(4, self.settings.indexnamevaluedelimiter).nth(3).unwrap_or("");
                self.undoWTS(jsonstr,tsu.clone());

            }     
        }

    }

    Ok(())
}

/////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////    

}


fn run() -> io::Result<()> {
    let jsonpath: &'static str = "C:/dev/jbloxdb/jbolxWorkspace/data/json.txt";

    let mut handler = jbothandler::new()?;

    let json_input = r#"
    {
        "key": "id,name",
        "keyobj": "user",
        "id": "user12345",
        "name": "Alice james",
        "email": "alice@example.com"
    }
    "#;

    let json_input_get1 = r#"
    {
        "key": "id,name",
        "keyobj": "user",
        "id": "user12345",
        "name": "Alice james",
        "email": "alice@example.com"
    }
    "#;

    // Call the method or block you want to time
    let start = Instant::now();

    handler.insertduplicate(json_input);
    handler.insertduplicate(json_input);
    handler.insertduplicate(json_input);
    handler.insertduplicate(json_input);
    handler.insert(json_input);
    handler.updateall(json_input);
    handler.update(json_input);
    handler.deleteall(json_input);
    handler.delete(json_input);
    handler.getall(json_input_get1);
    handler.get(json_input_get1);
    //new operator : undo
    handler.undo(json_input);

    let duration = start.elapsed();
    println!("Time taken: {:.2?}", duration);

    Ok(())
}
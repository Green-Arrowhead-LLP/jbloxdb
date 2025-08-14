// jbloxDB
// © 2025 Green Arrowhead LLP
// Licensed under the jbloxDB License v1.0
// See LICENSE.txt for terms.
// Free for individuals and small companies.
// Commercial license required for production use by companies over USD 5M revenue or for SaaS/product distribution.

use hyper::body::Body;
use memmap2::{MmapMut, MmapOptions};
use memmap2::{Mmap};
use memchr::memchr_iter;
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
use std::time::{Duration};
use std::error::Error;
use std::slice;

use serde_json::{json};

use std::env;
use std::fs;
use std::path::{Path};
use std::process;

use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{ Timelike};
use chrono::{DateTime};

use std::io::{BufRead, BufReader};

use std::io::{Read};

use std::sync::{Arc, Mutex};

use libc;

use memchr::memchr;

use std::thread;
use std::clone::Clone;
use serde_json::{ Number};

use std::collections::{BTreeMap};
use std::cmp::Ordering;
use std::borrow::Borrow;

use std::ops::Bound;
use std::ops::Bound::{Included, Excluded, Unbounded};

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
#[derive(Debug, Eq, PartialEq)]
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

} 
pub struct jbothandler {
    settings: Settings,
    file_size_map: HashMap<String, usize>, //file->file length
    //file_mmap_map: HashMap<String, MmapMut>,//file->mmep of file
    file_line_map: HashMap<String, HashMap<usize, usize>>,//file->pointer vector
    file_key_pointer_map: HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>, //file->key name -> key -> pointer
    datadir: String,
    logdir: String,
    log_file: BufWriter<File>,
    log_line_count : usize,
    state: Arc<Mutex<SharedState>>,
    stateR: Arc<Mutex<SharedStateR>>,
    size_digits: usize,


}

struct SharedState {
    file_mmap_map: HashMap<String, MmapMut>,
}
struct SharedStateR {
    file_mmap_map_forread: HashMap<String, Mmap>,
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
    ///
    /// - If both are JSON objects, their entries are merged/overwritten.
    /// - Otherwise, `master` is replaced by `patch`.
    pub fn merge_in_place_limited(master: &mut Value, patch: &Value) {
        match (master, patch) {
            (Value::Object(m), Value::Object(p)) => {
                for (k, pv) in p {
                    match m.get_mut(k) {
                        Some(mv) => merge_in_place(mv, pv),
                        None     => { m.insert(k.clone(), pv.clone()); }
                    }
                }
            }
            // For non‑objects (or mismatched types), just overwrite:
            (m_slot, p_slot) => {
                *m_slot = p_slot.clone();
            }
        }
    }

    /// Recursively merge `patch` into `master` in‑place.
    /// Special case: if `patch` is a string of the form "+=<n>", and `master` is a number,
    /// then add `<n>` to the original number instead of overwriting.
    fn merge_in_place(master: &mut Value, patch: &Value) {
        match (master, patch) {
            (Value::Object(m), Value::Object(p)) => {
                for (k, pv) in p {
                    match m.get_mut(k) {
                        Some(mv) => merge_in_place(mv, pv),
                        None => {
                            m.insert(k.clone(), pv.clone());
                        }
                    }
                }
            }
            (m_slot, p_slot) => {
                // Handle "+=<value>" addition syntax
                if let Value::String(s) = p_slot {
                    if let Some(rest) = s.strip_prefix("+=") {
                        // If master is a number, attempt addition
                        if let Value::Number(orig_num) = m_slot {
                            // Integer addition
                            if let (Some(orig_i), Ok(add_i)) = (orig_num.as_i64(), rest.parse::<i64>()) {
                                *m_slot = Value::Number(Number::from(orig_i + add_i));
                                return;
                            }
                            // Floating‑point addition
                            if let (Some(orig_f), Ok(add_f)) = (orig_num.as_f64(), rest.parse::<f64>()) {
                                *m_slot = Value::from(orig_f + add_f);
                                return;
                            }
                        }
                    }
                }
                // Fallback: overwrite with patch
                *m_slot = p_slot.clone();
            }
        }
    }


    /// Convenience wrapper: returns a fresh merged `Value` without mutating inputs.
    pub fn merge(master: &Value, patch: &Value) -> Value {
        let mut result = master.clone();
        merge_in_place(&mut result, patch);
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
    initfilesize: String,
    filesizemultiplier: f32, //for future use
    filepath: &str,
    recorddelimiter: &str,
    indexdelimiter: &str,
    indexnamevaluedelimiter: &str,
    enableviewdelete: bool,
    low_ram_mode: bool,
    MADVISE_CHUNK_S: usize,
    size_digits: usize,
    maxrecordlength: usize,
    maxrecords: usize,
    inputoffset: usize,
    file_size_map: &mut HashMap<String, usize>,
    file_mmap_map: &mut HashMap<String, memmap2::MmapMut>,
    file_mmap_map_forread: &mut HashMap<String, memmap2::Mmap>,
    file_line_map: &mut HashMap<String, HashMap<usize, usize>>,
    file_key_pointer_map: &mut HashMap<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>,
) -> std::io::Result<usize> {
    const TIMESTAMP_LEN: usize = 18;
    let path = Path::new(filepath);
    let mut offset: usize = inputoffset;

    if path.extension().and_then(|e| e.to_str()) == Some("jblox") {
        let fname = path.file_stem().unwrap().to_string_lossy().to_string();
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let filemetadata = file.metadata()?;

        let mut current_file_size = filemetadata.len() as usize;

        let mut configuredfilesize 
                    = value_with_overrides(&initfilesize, &fname);
        let initfilesizebytes =  configuredfilesize*1064*1064;
        //set to configured file size
        if(current_file_size < initfilesizebytes){
             file.set_len(initfilesizebytes as u64)?;
             current_file_size = initfilesizebytes;
        }
        let fileread =     file.try_clone()?;


        let mmapread = unsafe { MmapOptions::new().map(&fileread)? };
        let mut mmap: MmapMut = unsafe { MmapOptions::new().len(current_file_size).map_mut(&file)? };
        let base: PtrWrapper = PtrWrapper(UnsafeCell::new(mmap.as_mut_ptr()));
        let baseptr = base.as_ptr().addr();
        let lines = file_line_map.entry(fname.clone()).or_insert_with(HashMap::new);
        let keymap = file_key_pointer_map.entry(fname.clone()).or_insert_with(HashMap::new);

        let delim_u16 = if recorddelimiter.as_bytes().len() >= 2 {
            u16::from_le_bytes(recorddelimiter.as_bytes()[0..2].try_into().unwrap())
        } else {
            u16::from_le_bytes([recorddelimiter.as_bytes()[0], 0])
        };
        while offset < current_file_size {
            let size_start = offset + ((3 + TIMESTAMP_LEN + 1) * 2);
            let size_end = size_start + (size_digits * 2);
            if size_end > current_file_size {
                break;
            }

            let size_slice = &mmap[size_start..size_end];
            let size_utf16: Vec<u16> = size_slice.chunks(2)
                .map(|b| u16::from_le_bytes([b[0], b[1]])).collect();
            let size_str = String::from_utf16(&size_utf16).unwrap_or_else(|_| "0".to_string());

            let record_len: usize = size_str.trim().parse::<usize>().unwrap_or(0);
            if !size_str.trim().chars().all(|c| c.is_ascii_digit()) || record_len == 0 {
                break;
            }

            if offset + record_len > current_file_size {
                break;
            }

            //let recptr = PtrWrapper::from_base_and_offset(baseptr, offset);
            let recptr = offset;

            lines.insert(recptr, record_len);

            let mut i = size_end + 2;
            let record_end = offset + record_len;
            let mut key_u16 = Vec::with_capacity(128);
            while i + 1 < record_end {
                let u16_char = u16::from_le_bytes([mmap[i], mmap[i + 1]]);
                if u16_char == delim_u16 {
                    break;
                }
                key_u16.push(u16_char);
                i += 2;
            }

            if let Ok(key_field) = String::from_utf16(&key_u16) {
                for part in key_field.split(indexdelimiter) {
                    if let Some(dash_pos) = part.find(indexnamevaluedelimiter) {
                        let key_name = &part[..dash_pos];
                        let key_value = &part[dash_pos + 1..];
                        keymap.entry(key_name.to_string()).or_default()
                            .entry(NumKey(key_value.into())).or_default()
                            .push(recptr);
                    }
                }
            }

            offset += record_len;
        }

        file_mmap_map_forread.insert(fname.clone(), mmapread);
        file_mmap_map.insert(fname.clone(), mmap);
        file_size_map.insert(fname.clone(), offset);
    }

    Ok(offset)
}











////////////////////////////////////////////////////////////


impl jbothandler {


pub fn new_thread() -> io::Result<Self> {
    let config_path = get_config_path();
    println!("Config file path: {}", config_path.to_str().unwrap());
    let config = Config::builder()
        .add_source(config::File::with_name(config_path.to_str().unwrap()))
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config build error: {}", e)))?;

    let mut settings: Settings = config
        .try_deserialize()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config deserialize error: {}", e)))?;

        
    let datadir = settings.datadir.clone();
    let logdir = settings.logdir.clone();
    let configdir = config_path.to_str().unwrap();

    println!("config dir: {}", configdir.to_string());
    println!("log dir: {}", logdir);
    println!("datadir dir: {}", datadir);

    let log_filename = format!("{}/jblox.log", logdir);
    println!("log_filename : {}", log_filename);

    let log_file = BufWriter::new(OpenOptions::new().create(true).append(true).open(&log_filename)?);

    let log_path = std::path::Path::new(&log_filename);
    let mut log_line_count: usize = 0;
    if log_path.exists() {
        if let Ok(logfileforlines) = File::open(log_path) {
            let reader = BufReader::new(logfileforlines);
            log_line_count = reader.lines().count();
        }
    }

    let file_size_map = Arc::new(Mutex::new(HashMap::<String, usize>::new()));
    let file_mmap_map = Arc::new(Mutex::new(HashMap::<String, MmapMut>::new()));
    let file_mmap_map_forread = Arc::new(Mutex::new(HashMap::<String, Mmap>::new()));
    let file_line_map = Arc::new(Mutex::new(HashMap::<String, HashMap<usize, usize>>::new()));
    let file_key_pointer_map = Arc::new(Mutex::new(HashMap::<String, HashMap<String, BTreeMap<NumKey, Vec<usize>>>>::new()));

    let size_digits = {
        let max_length_digits = settings.maxrecordlength.to_string().len();
        if max_length_digits >= 4 {
            max_length_digits + 1
        } else {
            4
        }
    };

    for entry in fs::read_dir(datadir.clone())? {
        let path = entry?.path();
        if !(path.extension().and_then(|ext| ext.to_str()) == Some("jblox")) {
            continue;
        }

        let settings = settings.clone();
        let path_str = path.to_string_lossy().to_string();
        let file_name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("UNKNOWN").to_string();

        let mut currentoffset = 0;
        let maxrecord = 40_000;
        let mut currentrecordnum = 0;
        let mut keeprunning = true;
        let mut newrecordnum = 0;

        while keeprunning {
            let settings = settings.clone();
            let path_str = path_str.clone();
            let file_name_for_thread = file_name.clone(); // clone for this thread

            let file_size_map_thread = Arc::clone(&file_size_map);
            let file_mmap_map_forread_thread = Arc::clone(&file_mmap_map_forread);
            let file_mmap_map_thread = Arc::clone(&file_mmap_map);
            let file_line_map_thread = Arc::clone(&file_line_map);
            let file_key_pointer_map_thread = Arc::clone(&file_key_pointer_map);

            let handle = thread::spawn(move || {
                let offsetreturned = load_existing_file(
                    settings.initfilesize,
                    settings.newfilesizemultiplier,
                    &path_str,
                    settings.recorddelimiter.to_string().as_str(),
                    settings.indexdelimiter.to_string().as_str(),
                    settings.indexnamevaluedelimiter.to_string().as_str(),
                    settings.enableviewdelete,
                    settings.low_ram_mode,
                    settings.MADVISE_CHUNK,
                    size_digits,
                    settings.maxrecordlength,
                    maxrecord,
                    currentoffset,
                    &mut file_size_map_thread.lock().unwrap(),
                    &mut file_mmap_map_thread.lock().unwrap(),
                    &mut file_mmap_map_forread_thread.lock().unwrap(),
                    &mut file_line_map_thread.lock().unwrap(),
                    &mut file_key_pointer_map_thread.lock().unwrap(),
                ).unwrap();

                let newrecordnum = file_line_map_thread.lock().unwrap()
                    .get(&file_name_for_thread).unwrap().len();
                let mut size_map = file_size_map_thread.lock().unwrap();
                size_map.insert(file_name_for_thread.clone(), offsetreturned);

                println!(
                    "newrecordnum - currentrecordnum : {}, maxrecord: {}", 
                    newrecordnum - currentrecordnum, maxrecord
                );
            });

            handle.join().expect("Thread panicked"); // wait for thread

            // Update for next iteration
            newrecordnum = {
                let map = file_line_map.lock().unwrap();
                map.get(&file_name).unwrap().len()
            };

            if (newrecordnum - currentrecordnum) < maxrecord {
                keeprunning = false;
            } else {
                currentrecordnum = newrecordnum;
                currentoffset = {
                    let map = file_size_map.lock().unwrap();
                    *map.get(&file_name).unwrap()
                };
            }
        }

        println!("Finished processing file: {}, total number of records: {}", file_name,newrecordnum);
    }

    let state = Arc::new(Mutex::new(SharedState {
        file_mmap_map: Arc::try_unwrap(file_mmap_map).unwrap().into_inner().unwrap(),
    }));

    let stateR = Arc::new(Mutex::new(SharedStateR {
        file_mmap_map_forread: Arc::try_unwrap(file_mmap_map_forread).unwrap().into_inner().unwrap(),
    }));    
    println!("Ready to accept requests.");
    Ok(Self {
        settings,
        file_size_map: Arc::try_unwrap(file_size_map).unwrap().into_inner().unwrap(),
        file_line_map: Arc::try_unwrap(file_line_map).unwrap().into_inner().unwrap(),
        file_key_pointer_map: Arc::try_unwrap(file_key_pointer_map).unwrap().into_inner().unwrap(),
        datadir,
        logdir,
        log_file,
        log_line_count,
        state,
        stateR,
        size_digits,
    })
}


    pub fn new() -> io::Result<Self> {
        //get config file path
        let config_path = get_config_path();
        println!("Config file path: {}",config_path.to_str().unwrap());
        let config = Config::builder()
            .add_source(config::File::with_name(config_path.to_str().unwrap()))
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config build error: {}", e)))?;

        let mut settings: Settings = config
            .try_deserialize()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Config deserialize error: {}", e)))?;

        //directory to be used to store data
        let datadir = settings.datadir.clone();
        //log directory
        let logdir = settings.logdir.clone();
        println!("log dir: {}",logdir);
        println!("datadir dir: {}",datadir);

        //
        //log frequency in hours
        let logmaxlines: usize = settings.logmaxlines;
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
        let mut size_digits = 7; // Hard coded, to save space. limits size of each record to ~ 10MB


        let mut log_line_count: usize = 0;

        let log_filename = format!("{}/jblox.log", logdir);
        println!("log_filename : {}",log_filename);
        let log_file = BufWriter::new(
            OpenOptions::new().create(true).append(true).open(&log_filename)?
        );
        let log_path = std::path::Path::new(&log_filename);
        if log_path.exists() {
            if let Ok(logfileforlines) = File::open(log_path) {
                let reader = BufReader::new(logfileforlines);

                log_line_count = reader.lines().count();
            }
        }

        let mut file_size_map = HashMap::new();
        let mut file_mmap_map = HashMap::new();
        let mut file_mmap_map_forread = HashMap::new();
        let mut file_line_map: HashMap<String, HashMap<usize, usize>> = HashMap::new();
        let mut file_key_pointer_map = HashMap::new();



        for entry in std::fs::read_dir(datadir.clone())? {
        let mut currentoffset:usize = 0;
        let maxrecord: usize = 20000;
        let mut currentrecordnum = 0;
        let mut newrecordnum = 0;

            let path = entry?.path();

            if !(path.extension().and_then(|ext| ext.to_str()) == Some("jblox")) {
                continue;
            }

            let mut offsetreturned: usize = 0;
            let mut keeprunning : bool = true;

            //get file name without extension
            let file_name = path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("UNKNOWN");
            
            while(keeprunning) { //'0' would mean that all records are processed
                offsetreturned = load_existing_file(settings.initfilesize.clone(),
                    settings.newfilesizemultiplier,
                            &path.to_string_lossy().to_string(), 
                                        &recorddelimiter.to_string(), 
                                        &indexdelimiter.to_string(), 
                                        &indexnamevaluedelimiter.to_string(),
                                        settings.enableviewdelete,
                                        settings.low_ram_mode,
                                        settings.MADVISE_CHUNK,
                                        size_digits,
                                        settings.maxrecordlength,
                                        maxrecord,
                                        currentoffset,
                                        &mut file_size_map,
                                        &mut file_mmap_map,
                                        &mut file_mmap_map_forread,
                                        &mut file_line_map,
                                        &mut file_key_pointer_map,)?;

                newrecordnum = file_line_map.get(file_name).unwrap().len();

                currentoffset = file_size_map[file_name];

                //if currentoffset == offsetreturned, no more records so break
                println!("newrecordnum - currentrecordnum : {}, maxrecord: {}",newrecordnum -  currentrecordnum,maxrecord);
                if((newrecordnum -  currentrecordnum) <  maxrecord){
                    keeprunning = false;
                }
                else{
                    currentrecordnum = newrecordnum;
                }

            }
            let mut file_name_str = "";
            if let Some(file_name) = path.file_stem() {
                if let Some(s) = file_name.to_str() {
                    file_name_str = s;
                } else {
                    println!("Non-UTF8 file name, using fallback.");
                    file_name_str = "UNKNOWN";
                }
            } else {
                println!("Skipping path with no filename: {:?}", path);
            }

            if let Some(lines) = file_line_map.get(file_name_str) {
                println!("Total Number of records: {}",lines.len());

            } else {
                println!("No entry for {}", file_name_str);
            }

        }

        let state = Arc::new(Mutex::new(SharedState {
            file_mmap_map,
        }));
        let stateR = Arc::new(Mutex::new(SharedStateR {
            file_mmap_map_forread,
        }));


    println!("Ready to accept requests.");
        Ok(Self {settings,
                file_size_map, 
                file_line_map, 
                file_key_pointer_map, 
                datadir, 
                logdir, 
                log_file,
                log_line_count,
                state,
                stateR,
                size_digits,})
    }

    //Rank (0-based index by key order)
    //Returns how many keys are strictly less than the target (i.e., its position), 
    //or None if the key doesn’t exist.
    pub fn rank_of_key<V>(&mut self, map: &BTreeMap<NumKey, V>, key: &str) -> Option<usize> {
        if !map.contains_key(key) { return None; }
        // Count keys less than `key`. This is O(k) where k is #keys < target.
        let k = NumKey::from(key);
        Some(map.range(..k).count())
    }

    //Get by key (nice ergonomics)
    pub fn get_values<'a, V>(&mut self, map: &'a BTreeMap<NumKey, V>, key: &str) -> Option<&'a V> {
        map.get(key) // thanks to Borrow<str> impl
    }

    //Get key/value by index (the inverse)    
    pub fn nth_entry<'a, V>(&mut self, map: &'a BTreeMap<NumKey, V>, idx: usize)
        -> Option<(&'a NumKey, &'a V)>
    {
        map.iter().nth(idx)
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

pub fn append_line_and_track(&mut self, file_name: &str, new_line: &str, timestamp: &str) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use memmap2::MmapMut;

    let file_path = PathBuf::from(&self.datadir).join(format!("{}.jblox", file_name));

    // Format the full updated line with placeholder size for now
    let placeholder_size_str = "0".repeat(self.size_digits);
    let mut updated_line = format!("00-{timestamp}`{placeholder_size_str}:{new_line}\n");

    // Encode updated_line to UCS-2 to get actual byte length
    let ucs2_data: Vec<u16> = updated_line.chars()
        .map(|c| if c as u32 <= 0xFFFF { c as u16 } else { '?' as u16 })
        .collect();
    let line_bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(
            ucs2_data.as_ptr() as *const u8,
            ucs2_data.len() * 2,
        )
    };
    let total_size = line_bytes.len();

    // Now update size_str in updated_line with actual size
    let size_str = format!("{:0width$}", total_size, width = self.size_digits);
    updated_line = format!("00-{timestamp}`{size_str}:{new_line}\n");

    // Re-encode updated_line with correct size
    let ucs2_data: Vec<u16> = updated_line.chars()
        .map(|c| if c as u32 <= 0xFFFF { c as u16 } else { '?' as u16 })
        .collect();
    let line_bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(
            ucs2_data.as_ptr() as *const u8,
            ucs2_data.len() * 2,
        )
    };
    let line_len = line_bytes.len();

    let record_delim = self.settings.recorddelimiter.to_string();
    let index_delim = self.settings.indexdelimiter.to_string();
    let indexnamevalue_delim = self.settings.indexnamevaluedelimiter.to_string();

    {
        let mut state = self.state.lock().unwrap();
        let file_mmap_map_t: &mut HashMap<String, MmapMut> = &mut state.file_mmap_map;

        let mut stateR = self.stateR.lock().unwrap();
        let file_mmap_map_forread_t: &mut HashMap<String, Mmap> = &mut stateR.file_mmap_map_forread;

        if !file_mmap_map_t.contains_key(file_name) {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&file_path)?;
            let filesizenew = value_with_overrides(&self.settings.initfilesize, file_name);
            println!("initfilesize: {}", self.settings.initfilesize);
            let newfilesize = filesizenew * 1_000_000;
            file.set_len(newfilesize as u64)?;

            load_existing_file(
                self.settings.initfilesize.clone(),
                self.settings.newfilesizemultiplier,
                &file_path.to_string_lossy(),
                &record_delim,
                &index_delim,
                &indexnamevalue_delim,
                self.settings.enableviewdelete,
                self.settings.low_ram_mode,
                self.settings.MADVISE_CHUNK,
                self.size_digits,
                self.settings.maxrecordlength,
                0,
                0,
                &mut self.file_size_map,
                file_mmap_map_t,
                file_mmap_map_forread_t,
                &mut self.file_line_map,
                &mut self.file_key_pointer_map,
            );
        }
    }

    let mut state = self.state.lock().unwrap();
    let file_mmap_map_t = &mut state.file_mmap_map;
    let mmap = file_mmap_map_t.get_mut(file_name).unwrap();
    let start = *self.file_size_map.get_mut(file_name).unwrap();
    let end = start + line_len;
    if end > mmap.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Write exceeds mmap size",
        ));
    }

    mmap[start..end].copy_from_slice(line_bytes);

    *self.file_size_map.get_mut(file_name).unwrap() += line_len;

    let new_record_len = line_len;
    self.file_line_map.get_mut(file_name).unwrap().insert(start, new_record_len);

    if let Some(key_map) = self.file_key_pointer_map.get_mut(file_name) {
        let mut parts = updated_line.splitn(3, record_delim.as_str());
        parts.next();
        if let Some(key) = parts.next() {
            for part in key.trim().split(index_delim.as_str()) {
                if let Some(dash_pos) = part.find(indexnamevalue_delim.as_str()) {
                    let key_name = &part[..dash_pos];
                    let key_value = &part[dash_pos + 1..];
                    key_map.entry(key_name.to_string()).or_default()
                        .entry(NumKey(key_value.into())).or_default()
                        .push(start);
                }
            }
        }
    }

    let ratio = end as f64 / mmap.len() as f64;
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

pub fn print_line_forpointer_uselen(&self,file_name: &str, start_ptr_addr: usize, includedelete: bool) -> std::io::Result<String> {
    let mut retstr = "".to_string();
    let max_len: usize = self.settings.maxrecordlength;
    let lines = self.file_line_map.get(file_name.clone()).unwrap();
    

    let startptr = start_ptr_addr as *mut u8;

    let reclength = lines
                    .get(&start_ptr_addr)              // Option<&(usize,usize)>
                    .expect("missing entry");
    let mut u16_buffer: Vec<u16> = Vec::new();
    {
        let mut stateR = self.stateR.lock().unwrap();
        let file_mmap_map_forread_t = &mut stateR.file_mmap_map_forread;
        let mmap = file_mmap_map_forread_t.get(file_name).unwrap();

        let maptr = mmap.as_ptr().addr();
        let start = start_ptr_addr;
        let end = start + reclength;
        let bytes = &mmap[start..end];

        // 2) Must be even # of bytes
        assert!(bytes.len() % 2 == 0, "byte range must be 2-byte aligned");

        // 3) Turn every pair into a u16 code unit
        let code_units: Vec<u16> = bytes
            .chunks_exact(2)
            .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
            .collect();

        // 4) Decode (lossy to avoid panics on malformed)
        let line2 = String::from_utf16_lossy(&code_units);
        if includedelete && self.settings.enableviewdelete {
            retstr = line2.clone();
        } else if line2.starts_with("00") {
            retstr = line2.clone();
        } else {
            return Ok("".to_string());
        }
    }
    Ok(retstr.to_string())
}

pub fn print_line_forpointer2(&self, start_addr: usize, includedelete: bool) -> std::io::Result<String> {
    let max_len: usize = self.settings.maxrecordlength;
    let mut u16_buffer: Vec<u16> = Vec::new();

    for i in 0..max_len {
        unsafe {
            let start_ptr = start_addr as *mut u8;
            let byte1 = *start_ptr.add(i * 2);
            let byte2 = *start_ptr.add(i * 2 + 1);
            let u16_char = u16::from_le_bytes([byte1, byte2]);

            if u16_char == b'\n' as u16 {
                let line = String::from_utf16(&u16_buffer).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                })?;

                if includedelete && self.settings.enableviewdelete {
                    return Ok(line);
                } else if line.starts_with("00") {
                    return Ok(line);
                } else {
                    return Ok("".to_string());
                }
            } else {
                u16_buffer.push(u16_char);
            }
        }
    }

    if !u16_buffer.is_empty() {
        let line = String::from_utf16(&u16_buffer).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        if line.starts_with("00") {
            return Ok(line);
        }
    }

    Ok("".to_string())
}




////////////////////////////////////////////////////////////////// 


    pub fn print_line_forpointer_v01(&self, start_ptr: PtrWrapper, includedelete: bool) -> std::io::Result<String> {
        let max_len: usize = self.settings.maxrecordlength;
        let mut bytes = Vec::new();

        for i in 0..max_len {
            unsafe {
                let ch = *start_ptr.add(i);
                if ch == b'\n' {
                    // Convert collected bytes to UTF-8 string
                    let line = match std::str::from_utf8(&bytes) {
                        Ok(text) => text,
                        Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                    };

                    //Only return if line starts with "00"
                    //if includedelete is not true
                    if includedelete && self.settings.enableviewdelete {
                        return Ok(line.to_string());
                    } 
                    else if line.starts_with("00") {
                        return Ok(line.to_string());
                    } else {
                        return Ok("".to_string());
                    }
                } else {
                    bytes.push(ch);
                }
            }
        }

        // EOF reached without newline — try to process what we have
        if !bytes.is_empty() {
            let line = std::str::from_utf8(&bytes).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })?;

            if line.starts_with("00") {
                return Ok(line.to_string());
            }
        }

        Ok("".to_string())
    }

pub fn insert_duplicate_frmObject(&mut self, json: &Value,timestamp: &str) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};

    let file_name = json.get("keyobj")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;

    if let Some(obj) = json.as_object() {
        if let Some(Value::String(_key_name)) = obj.get("key") {
            // Step 2: Extract key value from JSON
            let key_str = {
                let pairs = self.extract_key_value_multiple(&json);
                let parts: Vec<String> = pairs
                    .into_iter()
                    .map(|(key, value)| {
                        let val_str = match value {
                            Value::String(s) => s.clone(),
                            _ => value.to_string().trim_matches('"').to_string(),
                        };
                        //replace indexnamevaluedelimiter with repindexnamevaluedelimiter
                        let mut key_t = key.clone().to_string();
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
                        format!("{}{}{}", key_t,self.settings.indexnamevaluedelimiter, val_str_t)
                    })
                    .collect();
                parts.join(&self.settings.indexdelimiter.to_string())
            };

            // Step 4: Minify JSON and prepare line
            let new_line = format!("{}:{}", key_str, json);

            // Step 5: Append to file
            self.append_line_and_track(file_name, &new_line,timestamp)?;
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid or missing 'key' or key value in JSON",
            ));
        }
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "JSON input is not an object",
        ));
    }

    Ok(())
}


    pub fn insertduplicate(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-insertduplicate-{}",currtstmp, compact_jsonstr))?;

        // Do not check for duplicates
        return self.insert_main(json_str, false, &currtstmp);
    }

    pub fn insert(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};
        
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-insert-{}",currtstmp, compact_jsonstr))?;

        // To check for duplicates (default behavior)
        return self.insert_main(json_str, true, &currtstmp);
    }

    pub fn insert_main(&mut self, json_str: &str, check_duplicates: bool, timestamp: &str) -> std::io::Result<Vec<String>> {
        use std::io::{Error, ErrorKind};
        
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;

        // Step 1: Parse the input JSON string into a Value.
        let json: Value = serde_json::from_str(&compact_jsonstr)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let file_name = json.get("keyobj")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing or invalid 'keyobj'"))?;

        let primkey_field_check = json.get("primkey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing or invalid 'primkey' field",
            ))?;

        if let Some(obj) = json.as_object() {
            if let Some(Value::String(key_name)) = obj.get("key") {

                //check duplicates
                //get key->keyvalue map
                let key_map = self.extract_keyname_value_map(&json)?;    
                // Call the existing method
                if check_duplicates {
                    //let key_map = self.extract_keyname_value_map(&json)?;
                    if let Ok(ptrs) = self.getmainforduplicatecheck(json_str,true,false) {
                        if !ptrs.is_empty() {
                            print!("Duplicate Records");
                            return Err(Error::new(ErrorKind::Other, "Duplicate Record."));
                        }
                    }
                }
                // Step 2: Extract key value from JSON
                let key_str = {
                    let pairs = self.extract_key_value_multiple(&json);
                    let parts: Vec<String> = pairs
                        .into_iter()
                        .map(|(key, value)| {
                            let val_str = match value {
                                Value::String(s) => s.clone(),
                                _ => value.to_string().trim_matches('"').to_string(),
                            };
                            //replace indexnamevaluedelimiter with repindexnamevaluedelimiter
                            let mut key_t = key.clone().to_string();
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

                let new_line = format!("{}:{}", key_str, json);

                // Step 5: Append to file
                self.append_line_and_track(file_name, &new_line,timestamp)?;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid or missing 'key' or key value in JSON",
                ));
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "JSON input is not an object",
            ));
        }
                    
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

    fn extract_key_value_multiple<'a>(&self, json: &'a Value) -> Vec<(&'a str, &'a Value)> {
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
        }

        results
    }

    fn extract_key_value<'a>(&self, json: &'a Value) -> Option<(&'a str, &'a Value)> {
        if let Some(obj) = json.as_object() {
            if let Some(Value::String(key_name)) = obj.get("key") {
                let key_name_str = key_name.as_str();

                if let Some(value) = obj.get(key_name_str) {
                    return Some((key_name_str, value));
                } else if let Some(found_value) = self.find_key_recursively(json, key_name_str) {
                    return Some((key_name_str, found_value));
                }
            }
        }
        None
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
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-delete-{}", currtstmp, compact_jsonstr))?;

        return self.deletemain(json_str, false); // delete last
    }

    pub fn deleteall(&mut self, json_str: &str) ->std::io::Result<Vec<String>> {

        let currtstmp: String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-deleteall-{}", currtstmp, compact_jsonstr))?;
 
       return self.deletemain(json_str, true); // delete all
    }

    pub fn deletemain(&mut self, json_str: &str, delete_all: bool) ->std::io::Result<Vec<String>> {
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


        //Step 4 A: Extract primary_key-value map from json
        let primkey_map: HashMap<String, String> = self.extract_prim_keyname_value_map(&json)?;
        
        if(primkey_map.len() == 0){
            Error::new(ErrorKind::InvalidInput, 
                "Missing Primary Key : 'primkey', mandatory for delete");
        }

        
        // Step 4 B: Extract key-value map from JSON
        let mut key_map: HashMap<String, String> = primkey_map.clone();

        //if primary key not present do all changes on the bases of key
        if(primkey_map.len() == 0){
            key_map = self.extract_keyname_value_map(&json)?;
        }
        // Step 5: Get matching pointers
        if let Some(ptrs) = self.get_fromkey_onlypointers(file_name, &key_map) {
            if ptrs.is_empty() {
                println!("Pointer list is empty.");
            } else if delete_all {
                for &ptr in &ptrs {
                    self.delete_using_pointer_fordelete(file_name, ptr);
                }
                println!("Total deleted records count: {} .", ptrs.len());
            } else {
                if let Some(&farthest_ptr) = ptrs.iter().max_by_key(|&&p| p as usize) {
                    self.delete_using_pointer_fordelete(file_name, farthest_ptr);
                    println!("Deleted last record.");
                }
            }
        } else {
            println!("No Record found.");
        }

        let lines = vec![
            String::from("done")
        ];
        Ok(lines)
    }

    pub fn delete_using_pointer_fordelete(&mut self, file_name: &str, addr: usize) {

        {
            let mut state = self.state.lock().unwrap();
            let file_mmap_map_t = &mut state.file_mmap_map;
            let mmap = file_mmap_map_t.get_mut(file_name).unwrap();

            mmap[addr]     = b'2'; 
            mmap[addr + 1] = 0x00;
            mmap.flush();

        }

    }

    pub fn delete_using_pointer_forupdate(&mut self, file_name: &str, addr:usize) {
        {
            let mut state = self.state.lock().unwrap();
            let file_mmap_map_t = &mut state.file_mmap_map;
            let mmap = file_mmap_map_t.get_mut(file_name).unwrap();

            mmap[addr]     = b'1'; 
            mmap[addr + 1] = 0x00;
            mmap.flush();

        }
    }
    //restores previous record. How far back depends on the undocount
    pub fn undo(&mut self, json_str: &str) ->std::io::Result<Vec<String>>{
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-update-{}",currtstmp, compact_jsonstr))?;

        return self.undomain(json_str, &currtstmp); // delete last
    }

    pub fn update(&mut self, json_str: &str) ->std::io::Result<Vec<String>>{
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-update-{}",currtstmp, compact_jsonstr))?;

        return self.updatemain(json_str, false,&currtstmp); // delete last
    }

    pub fn updateall(&mut self,json_str: &str) ->std::io::Result<Vec<String>> {
        let currtstmp:String = self.current_timestamp();
        let compact_jsonstr = self.compact_json_str(&json_str)?;
        self.log_message(&format!("{}-updateall-{}",currtstmp, compact_jsonstr))?;

       return self.updatemain(json_str, true,&currtstmp); // delete all
    }

    pub fn undomain(&mut self, json_str: &str,timestamp: &str) ->std::io::Result<Vec<String>> {
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

        //Step 4 A: Extract primary_key-value map from json
        let primkey_map: HashMap<String, String> = self.extract_prim_keyname_value_map(&json)?;

        //get undo count
        let undocount: i64 = if let Some(n) = json.get("undocount").and_then(|v| v.as_i64()) {
            if n < 1 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("'undocount' must be >= 1, got {}", n),
                ));
            }
            n
        } else {
            1 // default
        };

        if(primkey_map.len() == 0){
            Error::new(ErrorKind::InvalidInput, 
                "Missing Primary Key : 'primkey', mandatory for undo");
        }


        // Step 4 B: Extract key-value map from JSON
        let mut key_map: HashMap<String, String> = primkey_map.clone();



        // Step 5: Get matching pointers
        
        if let Some(ptrs) = self.get_fromkey_onlypointers(file_name, &key_map) {

            // collect into a mutable Vec (works whether ptrs is Vec or slice)
            let mut sortedptrs: Vec<usize> = ptrs.iter().copied().collect();

            // sort descending by address
            sortedptrs.sort_by(|a, b| ((*b as usize).cmp(&(*a as usize))));

            let farthest_ptr: usize = sortedptrs[sortedptrs.len()-1];


            if ptrs.is_empty() {
                println!("No record found - 2");
            }else {
                let mut currentundocount = 0 as i64;
                let mut undororigecord: String = String::new();
                let mut undorecpointer:  *mut u8 = std::ptr::null_mut();
                // inspect each line for every pointer
                for &ptr in sortedptrs.iter() {
                    match self.print_line_forpointer_uselen(file_name, ptr, true) {
                        Ok(line) => {
                            //last pointer will point to current record
                            if(currentundocount == 0){
                                if(line.starts_with('0')){
                                    currentundocount += 1;
                                    continue;
                                }
                                else{
                                    currentundocount = 1;
                                }
                            }
                            //get deleted/updated record only
                            if !line.starts_with('0') {
                               if(currentundocount == undocount){
                                //current count is equal to the count requested
                                //so, get the record
                                undororigecord = line.clone();
                                undorecpointer = ptr.clone() as *mut u8;
                                break;
                               }
                               else{
                                //increment current count, till undocount
                                    currentundocount += 1;
                               }
                            }
                            // replace this with whatever inspection logic you need
                            //println!("Pointer {:?} -> line: {}", ptr, line);
                        }
                        Err(e) => {
                            Error::new(ErrorKind::InvalidInput, 
                                "No record found");
                        }
                    }
                }
                if(currentundocount < undocount 
                    || 
                    undororigecord.is_empty() 
                    || 
                    undororigecord.len() == 0){
                            let errorstr = format!("No record found, old records count: {} ", currentundocount);
                            Error::new(ErrorKind::InvalidInput, 
                                errorstr);
                }

                //exract json data
                let origjson = extract_json(&undororigecord,self.settings.recorddelimiter);

                //delete orig record
                self.delete_using_pointer_forupdate(file_name, farthest_ptr);
                
                self.insert_duplicate_frmObject(&origjson, timestamp);

                println!("Undo complete.");
            }

        } else {
            println!("No Record found - 3");
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

        //Step 4 A: Extract primary_key-value map from json
        let primkey_map: HashMap<String, String> = self.extract_prim_keyname_value_map(&json)?;
            
        // Step 4 B: Extract key-value map from JSON
        let mut key_map: HashMap<String, String> = primkey_map.clone();

        //if primary key not present do all changes on the bases of key
        if(primkey_map.len() == 0){
            key_map = self.extract_keyname_value_map(&json)?;
        }

        // Step 5: Get matching pointers
        if let Some(ptrs) = self.get_fromkey_onlypointers(file_name, &key_map) {
            if ptrs.is_empty() {
                println!("Pointer list is empty.");
            } else if update_all {
                for &ptr in &ptrs {
                    let origrec = self.print_line_forpointer_uselen(file_name,ptr,false)?;

                    //exract json data
                    let origjson = extract_json(&origrec,self.settings.recorddelimiter);
                    let modjson: Value = json.clone();
                    if(!origjson.is_null()){
                        //merge json (update json) with orig json data
                        let mergedjson = merge(&origjson, &modjson);

                        //delete orig record     
                        self.delete_using_pointer_forupdate(file_name, ptr);

                        //insert merged json 
                        self.insert_duplicate_frmObject(&mergedjson, timestamp);  
                    }
                  

                }
                println!("Deleted {} records.", ptrs.len());
            } else {
                if let Some(&farthest_ptr) = ptrs.iter().max_by_key(|&&p| p as usize) {

                    let origrec = self.print_line_forpointer_uselen(file_name,farthest_ptr,false)?;

                    //exract json data
                    let origjson = extract_json(&origrec,self.settings.recorddelimiter);
                    let modjson: Value = json.clone();

                    //merge json (update json) with orig json data
                    let mergedjson = merge(&origjson, &modjson);

                    //delete orig record
                    self.delete_using_pointer_forupdate(file_name, farthest_ptr);
                    
                    //insert merged json 
                    self.insert_duplicate_frmObject(&mergedjson, timestamp);

                    println!("Deleted last record.");
                }
            }

        } else {
            println!("No Record found.");
        }

        let keyname = json.get("key")
            .and_then(|v| v.as_str())
            .and_then(|s| s.split(',').next())
            .map(str::trim)
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "'key' field missing or invalid in JSON",
            ))?;

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

    //apply AND between the keys
    pub fn getmainforduplicatecheck(&mut self, 
                    json_str: &str,
                    use_intersection: bool,
                    includedelete: bool) -> std::io::Result<Vec<String>> {

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
        let recstart = "0";
        //Extract primary_key-value map from json
        let primkey_map: HashMap<String, String> = self.extract_prim_keyname_value_map(&json)?;
        if(primkey_map.len() == 0){
            Error::new(ErrorKind::InvalidInput, 
                "Missing Primary Key : 'primkey', mandatory for insert");
        }

        
        //get key->keyvalue map
        // Call the existing method
        let lines = self.get_fromkey_forduplicate(file_name, &primkey_map, &recstart, use_intersection,includedelete,false);
        for line in &lines {
            println!("Line: {}", line);
            //need to print just one record, no need to print all
            break;
        }
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
        let lines = self.get_fromkey(file_name, &key_map, &recstart, use_intersection,includedelete,reverse,maxrecordscount);
        Ok(lines)

    }

    pub fn get_fromkey_forduplicate(
        &mut self,
        file_name: &str,
        key_map: &HashMap<String, String>,
        recstart: &str, //if "0" send records from begining, else start from this record (including)
        use_intersection: bool,
        includedelete: bool,
        reverse: bool, //currently ingnored
    ) -> Vec<String> {

        let mut result: Vec<usize> = Vec::new(); // ✅ Flat vector
        let mut found_keystart = recstart == "0";

        if !use_intersection{
            let mut seen = HashSet::new();

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
        let mut count = 0;
        for pw in &result {
            // Convert Err(_) → empty string
            let line = match self.print_line_forpointer_uselen(file_name,pw.clone(), false) {
                Ok(s) => s,
                Err(_) => String::new(),
            };
            //println!("record found: {}",line);
            if !line.is_empty() {
                count += 1;
                break;
            }
        }

        let mut result_lines: Vec<String> = Vec::new();

        if(count > 0){
            let meta_json_str = format!(r#"{{"meta":{{"total_count":{}}}}}"#, ">0");
            result_lines.push(meta_json_str.clone());        
        }
        return result_lines;
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
            panic!("bad op in condition: '{cond}' (expected one of: >=, <=, >, <)");
        };

        if key_str.is_empty() {
            panic!("empty key after operator in condition: '{cond}'");
        }

        // Build the range iterator based on the operator
        let iter = match op {
            "<"  => val_map.range::<str, _>((Unbounded, Excluded(key_str))),
            "<=" => val_map.range::<str, _>((Unbounded, Included(key_str))),
            ">"  => val_map.range::<str, _>((Excluded(key_str), Unbounded)),
            ">=" => val_map.range::<str, _>((Included(key_str), Unbounded)),
            _    => unreachable!("op already validated"),
        };

        // Flatten all Vec<usize> values into a single Vec<usize>
        iter.flat_map(|(_, v)| v.iter().copied()).collect()
    }


    pub fn get_fromkey(
        &mut self,
        file_name: &str,
        key_map: &HashMap<String, String>,
        recstart: &str, //if "0" send records from begining, else start from this record (including)
        use_intersection: bool,
        includedelete: bool,
        reverse: bool,   //send records in decending
        maxrecordscount: usize, //max number of records to return
    ) -> Vec<String> {

        let mut result: Vec<usize> = Vec::new(); // ✅ Flat vector
        let mut found_keystart = recstart == "0";

        if !use_intersection{
            let mut seen = HashSet::new();

            for (keyname, keyvalue) in key_map {

                if let Some(ptrmap) = self.file_key_pointer_map.get(file_name) {
                    if let Some(val_map) = ptrmap.get(keyname) {
                        if(keyvalue.starts_with(&self.settings.notoperator)){
                            let newkeyvalue 
                                = keyvalue.strip_prefix(&self.settings.notoperator).unwrap_or(keyvalue);
                            for (key1, vec1) in val_map {
                                if key1 != &NumKey(newkeyvalue.into())  {
                                    for ptr1 in vec1 {
                                        let rawptr1 = ptr1; // Use raw pointer for uniqueness
                                        if seen.insert(rawptr1) {
                                            result.push(ptr1.clone());
                                        }
                                    }
                                }
                            }
                        }
                        else if(keyvalue.starts_with("min()"))
                        {
                            let mut results_vec: Vec<usize> = Vec::new();

                            if let Some((max_key, last_vals)) = val_map.iter().next() {
                                results_vec.extend_from_slice(last_vals);
                            }                            

                            //find union of results_vec and result
                            let mut seen: HashSet<_> = result.iter().copied().collect();
                            for val in results_vec {
                                if seen.insert(val) { // insert returns false if already present
                                    result.push(val);
                                }
                            }

                        }
                        else if(keyvalue.starts_with("max()"))
                        {
                            let mut results_vec: Vec<usize> = Vec::new();

                            if let Some((max_key, last_vals)) = val_map.iter().next_back() {
                                results_vec.extend_from_slice(last_vals);
                            }                            

                            //find union of results_vec and result
                            let mut seen: HashSet<_> = result.iter().copied().collect();
                            for val in results_vec {
                                if seen.insert(val) { // insert returns false if already present
                                    result.push(val);
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
                            let mut seen: HashSet<_> = result.iter().copied().collect();
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
        else{
            let mut is_first = true;

            for (keyname, keyvalue) in key_map {
                if let Some(ptrmap) = self.file_key_pointer_map.get(file_name) {
                    if let Some(val_map) = ptrmap.get(keyname) {
                        if(keyvalue.starts_with(&self.settings.notoperator)){
                            let newkeyvalue 
                                = keyvalue.strip_prefix(&self.settings.notoperator).unwrap_or(keyvalue);
                            
                            let mut ptrsfound =false;
                            for (key1, vec1) in val_map {

                                if key1 != &NumKey(newkeyvalue.into()) {


                                    if is_first {
                                        result = vec1.iter().cloned().collect();
                                        is_first = false;
                                    } else {
                                        result.retain(|ptr| {
                                            vec1.iter().any(|p| p == ptr)
                                        });

                                    }
                                    ptrsfound = true;  
                                }                               

                            }
                            if(!ptrsfound){
                                result.clear();
                            }
                            if result.is_empty() {
                                break; // Intersection became empty -> stop
                            }

                        }
                        else if(keyvalue.starts_with("min()"))
                        {
                            let mut results_vec: Vec<usize> = Vec::new();

                            if let Some((max_key, last_vals)) = val_map.iter().next() {
                                results_vec.extend_from_slice(last_vals);
                            }                            

                            //now find intersection
                            if is_first {
                                result = results_vec.clone();
                                is_first = false;
                            }
                            else{
                                result = self.intersection_unique_unordered(&results_vec, &result);
                            } 

                        }
                        else if(keyvalue.starts_with("max()"))
                        {
                            let mut results_vec: Vec<usize> = Vec::new();

                            if let Some((max_key, last_vals)) = val_map.iter().next_back() {
                                results_vec.extend_from_slice(last_vals);
                            }                            

                            //now find intersection
                            if is_first {
                                result = results_vec.clone();
                                is_first = false;
                            }
                            else{
                                result = self.intersection_unique_unordered(&results_vec, &result);
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
                                result = results_vec.clone();
                                is_first = false;
                            }
                            else{
                                result = self.intersection_unique_unordered(&results_vec, &result);
                            } 

                        }
                        else if(keyvalue.contains(",")){
                            let mut seen = HashSet::new();
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
                                result = self.intersection_unique_unordered(&resultForOr, &result);
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
        if(reverse){
            result.sort_by(|a, b| b.cmp(a));
        }
        else{
            result.sort();
        }
        let mut result_lines: Vec<String> = Vec::with_capacity(self.settings.maxgetrecords + 1);
        //add meta data
        let total_records = result.len(); // your value
        let meta_json_str = format!(r#"{{"meta":{{"total_count":{}}}}}"#, total_records);
        result_lines.push(meta_json_str.clone());
        let mut actualreccound : usize = 0;

        let mut recordsadded: usize = 0;


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
                        actualreccound += 1;
                    }
                } else {
                        if result_lines.len() < maxrecordscount {
                            result_lines.push(line.to_string());
                        }
                        actualreccound += 1;
                }
            }
        }

        //reset count
        result_lines[0] = format!(r#"{{"meta":{{"total_count":{}}}}}"#, actualreccound);

        if result_lines.is_empty() {
            //eprintln!("No matching records found.");
            return Vec::new();
        }

        //println!("Got the matching pointers");

        result_lines
    }


    pub fn get_fromkey_onlypointers(
        &self,
        file_name: &str,
        key_map: &HashMap<String, String>,
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

    pub fn extract_prim_keyname_value_map(&self, json: &Value) -> std::io::Result<HashMap<String, String>> {
        let mut map = HashMap::new();

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

    pub fn extract_keyname_value_map(&self, json: &Value) -> std::io::Result<HashMap<String, String>> {
        let mut map = HashMap::new();

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

    pub fn log_message(&mut self, message: &str) -> std::io::Result<()> {
        writeln!(self.log_file, "{}", message)?;
        println!("message: {}", &message.chars().take(self.settings.maxlogtoconsolelength).collect::<String>());
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
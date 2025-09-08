jbloxDB
--------

A zero-metadata configuration, high-performance JSON data management system developed in Rust.

jbloxDB supports memory-mapped file operations, pointer indexing, and high-speed data retrieval by allowing direct pointer access to large file-based records.

It is completely modular. The installation includes the core database engine and a sample HTTP server wrapper. Users can easily apply a security layer around the included HTTP server or create their own secure communication layer. The small footprint of jbloxDB makes it easy to embed in existing software applications.

Developed by: Green Arrowhead LLP  
Year: 2025

--------

Key Features:
- High-speed access to structured records in JSON format
- Zero-metadata configuration — no need to define tables, columns, keys, etc.
- Modular design — easy to build custom communication, security, or wrapper layers
- Small footprint
- Text-based data storage files
- Open source
- Rust-native performance
- Full UCS-2 support
- All requests and responses in JSON formats.

--------

Installation:

GitHub:   `https://github.com/Green-Arrowhead-LLP/jbloxdb`

--------

Usage:

### Start jbloxDB:

(everytime jbloxDB runs, it creates jblox.lck file, in the current directory, to make sure only one instance is running at any given point of time. So, before running jbloxDB, make sure there are no other instances of jbloxdb running, and remove jblox.lck if you need to.)

Microsoft Window:
Prerequisite: 
1. Make sure 'config' directory, containing jbloxsettings.toml and jbloxhttpsettings.toml, exists at the same location as jbloxdb.exe . Make necessary changed to 'ip' in jbloxhttpsettings.toml if required. (if config directory is not found, jbloxDB will check the parent directory, and so on, till it reaches base directory)
2. Make sure data; log; html directories mentioned in the toml files exist.

Execute: jbloxdb.exe 

Linux/Linux based OS

1. Make sure 'config' directory, containing jbloxsettings.toml and jbloxhttpsettings.toml,exists at the same location as jbloxdb . Make necessary changed to 'ip' in jbloxhttpsettings.toml if required.(if config directory is not found, jbloxDB will check the parent directory, and so on, till it reaches base directory)
2. Make sure data; log; html directories mentioned in the toml files exist.

Execute: ./jbloxdb

### Stop jbloxDB:

jbloxDB check for jblox.stop file, at the same location as jbloxdb executable, every 60 seconds. If file exists, jbloxDB will stop taking any new http requests and wait for 10 seconds to shutdown jbloxDB.


### 📘 Request Format:

```json
{
  "op": "<operation>",
  "data": {
    "primkey": "<comma-separated primary keys: only for insert; update; delete; undo>",
    "key": "<comma-separated keys>",
    "keyobj": "<object name: each keyobj has unique data file>",
    "recstart": "<record ID for pagination>",
    "id": "user12345",
    "name": "Alice James",
    "email": "alice@example.com"
  }
}
```

- `op` – required operation: `get`, `getall`, `insert`, `insertduplicate`, `update`, `updateall`, `delete`, `deleteall`, `undo`
- `primkey` – Elements in JSON for unique identification. This field is necessary for insert(all); update(all); delete(all); undo operations. Primary key element(s) cannot be modified for a JSON and will be ignored if present in 'update' operation. 
- `key` – the key(s) used for indexing; must match keys present in the JSON data. JSON records could only be searched (and sorted) based on 'key' and 'primkey' elements. Elements can be added/removed from `key` using update operation.
- `keyobj` – the logical object being stored; each object is stored in a separate file. Multiple object types, with completely different JSON representation, could however be saved in same file using same keyobj.
- `recstart` – optional; used for pagination by providing the record ID from which to continue fetching

### ⚙ Operation Details:

- **get**: Fetches records matching all keys (AND logic); paginated via `recstart`*
- **getall**: Fetches records matching any of the keys (OR logic)
- **gethist**: Fetches records' history i.e. all records: deleted or updated, matching all keys (AND logic)
- **gethistall**: Fetches records' history i.e. all records: deleted or updated, matching any of the keys (OR logic)
- **getdesc**: Fetches records, in descending order, matching all keys (AND logic)
- **getalldesc**: Fetches records, in descending order, matching any of the keys (OR logic)
- **gethistdesc**: Fetches records' history i.e. all records: deleted or updated, in descending order,  matching all keys (AND logic)
- **gethistalldesc**: Fetches records' history i.e. all records: deleted or updated, in descending order, matching any of the keys (OR logic)
- **insert**: Inserts a new JSON record; duplicates not allowed
- **insertduplicate**: Inserts even if a record with matching keys already exists
- **update**: Marks old record as deleted, then inserts new one (requires full record)
- **updateall**: Deletes all matching records before inserting new one
- **delete**: Marks the latest matching record as deleted
- **deleteall**: Deletes all matching records
- **undo**: Restores previous JSON and mark current one as deleted. By default, last record with matching `primkey` is restored, this however can be changed through use of `undocount` which will Deletes all matching records
*pagination via `recstart` is available to all 'get' requests
--------

Data Storage:

jbloxDB stores data in text files, with a separate file for each `keyobj`. No joins are supported across objects.

Each record format:
<status>-<timestamp>:<key1>-<val1>`<key2>-<val2>:<raw JSON>

Status codes:
- `00` – valid
- `10` – deleted by delete/deleteall
- `20` – deleted by update/updateall

--------

Configuration Files:

- `jbloxsettings.toml` – core engine parameters
- `jbloxhttpsettings.toml` – HTTP server settings

All file sizes, growth increments, record limits, and delimiters are configurable.

--------

Logging:

All requests are logged. You can configure the log file location and the number of records before rotation.

--------

Examples:

**Insert:**
```json
{
  "op": "insert",
  "data": {
    "key": "user_id,dob",
    "keyobj": "customer",
    "user_id": "USR30495821",
    "name": {
      "first": "Jordan",
      "last": "Reynolds"
    },
    "dob": "1992-04-18",
    "email": "jordan.reynolds@examplemail.com",
    "phone": "+1-303-555-0198",
    "address": {
      "line1": "728 West Elm Street",
      "line2": "Apt 4B",
      "city": "Denver",
      "state": "Colorado",
      "postal_code": "80204",
      "country": "USA"
    },
    "kyc_status": "Verified"
  }
}
```

**Update:** *(Must include full record, not just updated fields)*
```json
{
  "op": "update",
  "data": {
    "key": "user_id,dob",
    "keyobj": "customer",
    "user_id": "USR30495821",
    "name": {
      "first": "Jordan",
      "last": "Reynolds"
    },
    "dob": "1992-04-18",
    "email": "jordan.reynolds@examplemail.com",
    "phone": "+1-344-999-2000",
    "address": {
      "line1": "728 West Elm Street",
      "line2": "Apt 4B",
      "city": "Denver",
      "state": "Colorado",
      "postal_code": "80204",
      "country": "USA"
    },
    "kyc_status": "Verified"
  }
}
```

**Delete:**
```json
{
  "op": "delete",
  "data": {
    "key": "user_id",
    "keyobj": "customer",
    "user_id": "USR30495821"
  }
}
```

**Get:**
```json
{
  "op": "get",
  "data": {
    "key": "dob",
    "keyobj": "customer",
    "dob": "1992-04-18"
  }
}
```

**Get with Pagination:**
```json
{
  "op": "get",
  "data": {
    "key": "dob",
    "keyobj": "customer",
    "recstart": "00-020724",
    "dob": "1992-04-18"
  }
}
```

--------

Customization:

The included HTTP server is basic and does not support HTTPS.  
We recommend:
- Creating your own HTTPS wrapper
- Or placing the HTTP server behind a secure reverse proxy

Note: jbloxDB provides no built-in authentication or authorization. Your wrapper should handle security.

jbloxDB is wrapper-agnostic — you can build custom wrappers using HTTP, messaging queues, etc.

--------

## License:

This project is licensed under the **jbloxDB License v1.0**, based on the Elastic License v2.

- Free for individuals in all environments
- Free for companies with revenue < USD 5 million in all environments
- Companies with revenue ≥ USD 5 million must license for production use
- Companies bundling or hosting jbloxDB as a service must license it commercially

See `LICENSE.txt` for full terms.

--------

Contact:

For commercial licensing and support:

Green Arrowhead LLP  
Email: contact@greenarrowhead.com  
Website: https://www.jbloxdb.com

--------

© 2025 Green Arrowhead LLP. All rights reserved.

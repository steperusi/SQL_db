# SimpleDB

**SimpleDB** is a lightweight, educational database engine written in C for Windows. It uses a custom binary format and stores data persistently using a B-tree structure (with internal and leaf nodes). It supports basic SQL-like commands for inserting and selecting data.

---

## Features

- Persistent storage using binary paging
- Custom B-tree implementation with internal and leaf nodes
- SQL-like support for:
  - `insert <id> <username> <email>`
  - `select`
- Meta commands:
  - `.exit` to quit
  - `.btree` to view the B-tree
  - `.constants` to print storage constants
- Error handling for:
  - Duplicate keys
  - Syntax errors
  - String length violations

---

## Requirements

- Windows operating system
- C compiler (MSVC or MinGW)
- Terminal/console that supports standard input/output

---

## Build Instructions (Windows Only)

```bash
gcc -o simpledb simpledb.c
```

---

## Usage

```bash
simpledb.exe test.db
```

---

## Commands

### SQL-like Commands

```sql
insert <id> <username> <email>
select
```

### Meta Commands

```bash
.exit
.btree
.constants
```

---

## Example Session

```text
db > insert 1 alice alice@example.com
Executed.

db > insert 2 bob bob@example.com
Executed.

db > select
(1, alice, alice@example.com)
(2, bob, bob@example.com)
Executed.

db > .btree
Tree:
- leaf (size 2)
  - 1
  - 2

db > .exit
```

---

## Code Structure

- `Row`: Represents one database row
- `Pager`: Manages reading and writing fixed-size pages to disk
- `Cursor`: Used to traverse rows in the B-tree
- `Table`: Represents the database and holds the pager and root page
- `B-tree Nodes`:
  - Leaf nodes contain actual data
  - Internal nodes act as navigation points

---

## Limitations

- Windows-only
- Only supports `insert` and `select`
- One hardcoded table structure (`users`)
- No SQL parser — requires strict syntax
- Fixed page and row sizes
- Maximum of 100 pages in memory

---

## Educational Purpose

SimpleDB is designed for learning purposes. It demonstrates:

- How B-trees store and retrieve data
- Low-level memory alignment and serialization
- File paging and on-disk persistence
- The foundational design of relational databases

---

## License

MIT License

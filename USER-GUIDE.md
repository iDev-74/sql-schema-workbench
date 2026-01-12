# User Guide

This is a short, practical guide to using the tool effectively. It is designed to be skimmed once and referenced only when needed.

---

## Quick Start

1. Launch the application
2. Connect to a database
3. Open the schema view
4. Test queries safely
5. Return to your primary work

You should be productive within minutes.

---

## Core Rules

- All queries are **read-only**
- Unsafe operations are blocked and explained
- The schema is the source of truth
- Limits are intentional and predictable

If something is blocked, it is by design.

---

## Common Tasks

### Understand an Unfamiliar Database

1. Connect to the database
2. Browse tables and columns
3. Review relationships
4. Preview small samples of data

Goal: build a correct mental model quickly.

---

### Test SQL Safely

1. Write or paste a SELECT query
2. Run it in the SQL tester
3. Review results or feedback

You can experiment without fear of modifying data.

---

### Find Where a Value Lives

1. Use Logic by Example
2. Enter known values (IDs, names, codes)
3. Review suggested tables and columns

Searches are limited to remain fast and safe.

---

### Build a First-Pass Query

1. Use the query builder
2. Review inferred tables and joins
3. Send the query to the SQL tester

Generated SQL is a starting point, not a final answer.

---

## Utilities

Small offline tools are included to reduce context switching.

- RegEx testing
- Pattern explanation

These tools are optional and independent of database access.

---

## Privacy and Safety

- Runs entirely locally
- No telemetry or tracking
- No background network activity
- Credentials exist only in session memory

---

## When Not to Use This Tool

- Production writes
- Automated pipelines
- Large-scale analytics
- Visualization-driven exploration

This tool is designed to support thinking, not automation.


# Openplace Bug Report

**Date:** 2025-11-10
**Test Environment:** Local testing with live PLACE website

---

## Critical Bugs

### 1. **Website Access Returns 403 Forbidden**
**Severity:** CRITICAL
**File:** `openplace/tasks/scrape/navigate.py:44`
**Status:** Blocks entire pipeline

**Description:**
The PLACE website (`https://www.marches-publics.gouv.fr`) returns HTTP 403 when accessed by the scraper. This blocks the entire discovery workflow.

**Error:**
```
DEBUG:urllib3.connectionpool:https://www.marches-publics.gouv.fr:443 "GET /?page=Entreprise.EntrepriseAdvancedSearch&AllCons HTTP/1.1" 403 13
AssertionError: 403
```

**Testing:**
- Tested with curl: 403
- Tested with User-Agent headers: 403
- Tested with Python requests: 403

**Possible Causes:**
- Bot protection/WAF (Web Application Firewall)
- IP-based rate limiting or blocking
- Cloudflare or similar protection service
- Website structure changes

**Recommendations:**
1. Add User-Agent header to all requests
2. Add request delays/rate limiting
3. Investigate if the website has changed its structure
4. Check if GitHub Actions runners can access the site (they may have different IPs)
5. Consider using sessions with proper cookies
6. May need to implement retry logic with exponential backoff

---

### 2. **Parent-Child Relationship Bug in ZIP Archive Entries**
**Severity:** HIGH
**File:** `openplace/storage/local/queries.py:171`
**Function:** `create_zip_entries()`

**Description:**
When creating archive entries from ZIP files, the parent_id is set using `parent_entry.id`, but this ID is None because the parent entry hasn't been committed to the database yet. This breaks the file tree structure.

**Code:**
```python
parent_entry = next((e for e in entries if e.path == str(parent_path)), None)
entry = ArchiveEntry(
    name=name,
    path=normalized_path,
    parent_id=parent_entry.id if parent_entry else None,  # BUG: parent_entry.id is None
    posting_id=posting_id,
    is_dir=is_dir,
    is_extracted=False,
)
```

**Impact:**
- All archive entries will have `parent_id=None`
- File tree hierarchy is lost
- Cannot reconstruct directory structure from database

**Fix Required:**
- Need to commit entries in batches and retrieve their IDs
- OR build a map of path -> entry after all entries are created and committed
- OR use a different strategy for tracking parent-child relationships

---

### 3. **Incorrect Optional Path Handling in CLI**
**Severity:** MEDIUM
**File:** `openplace/cli.py:159`
**Function:** `export_archive_content()`

**Description:**
The logic for handling optional output_file parameter is incorrect. Using `or` with Path objects doesn't work as expected because Path objects are always truthy.

**Code:**
```python
output_path = Path(output_file) or Path(f"archive_content_{archive_content_id}.txt")
```

**Current Behavior:**
- If `output_file` is None, `Path(None)` creates `Path('None')` (a Path to a file literally named "None")
- The `or` operator never evaluates the right side because Path objects are truthy

**Expected Behavior:**
- If `output_file` is None, should use default filename
- If `output_file` is provided, should use that filename

**Fix Required:**
```python
output_path = Path(output_file) if output_file else Path(f"archive_content_{archive_content_id}.txt")
```

---

## Medium Issues

### 4. **Naming Conflict: ArchiveContent**
**Severity:** MEDIUM
**Files:**
- `openplace/storage/local/models.py` (SQLModel)
- `openplace/tasks/store/types.py` (dataclass)

**Description:**
There are two different classes named `ArchiveContent`:
1. A SQLModel database model in `storage/local/models.py`
2. A dataclass in `tasks/store/types.py`

This creates confusion and potential import errors depending on which one is imported.

**Current Usage:**
- `markdown.py` imports the dataclass version from `tasks.store.types`
- `queries.py` uses the SQLModel version
- Different fields between the two classes

**Impact:**
- Code confusion
- Potential bugs if wrong class is imported
- Makes codebase harder to maintain

**Recommendations:**
- Rename one of them (e.g., `ArchiveContentDTO` for the dataclass)
- Or consolidate into a single class if possible
- Add clear documentation about when to use which

---

### 5. **CLI Flag Conflict**
**Severity:** LOW
**File:** `openplace/cli.py:126`

**Description:**
Both `--filename-date` and `--debug` use the same short flag `-D` in the `export_archives` command.

**Code:**
```python
filename_date: bool = Option(False, "--filename-date", "-D", ...)
debug: bool = Option(False, "--debug", "-D", ...)
```

**Impact:**
- Only one of these flags will work
- User confusion
- Typer will likely raise an error or ignore one

**Fix Required:**
- Change one of the short flags (e.g., use `-T` for timestamp/date)

---

## Code Quality Issues

### 6. **Inconsistent Async Usage**
**Severity:** LOW
**File:** `openplace/tasks/extract/markdown.py`

**Description:**
The `extract_markdown()` function is declared as async but doesn't use any await operations. MarkItDown's `convert()` method is synchronous.

**Code:**
```python
async def extract_markdown(archive_path: Path, persist: bool = True) -> ArchiveContent | None:
    archive_content = md.convert(archive_path).markdown  # Synchronous call
```

**Impact:**
- No performance benefit from async/await
- False expectation of concurrency
- May cause issues if MarkItDown blocks on I/O

**Recommendations:**
- Either make the function truly async (if MarkItDown supports it)
- Or remove async and use multiprocessing/threading for parallel processing
- Or keep as-is but document that it's "fake async" for compatibility

---

### 7. **Missing Error Handling in PlacePostingIterator**
**Severity:** MEDIUM
**File:** `openplace/tasks/scrape/navigate.py`

**Description:**
The iterator has minimal error handling. Network failures, timeouts, or unexpected responses could crash the entire discovery process.

**Issues:**
- Line 44: Hard assertion on status code (no retry logic)
- Line 78: Another hard assertion
- No handling of temporary network failures
- No retry logic for transient errors

**Recommendations:**
- Add retry logic with exponential backoff
- Better error messages
- Graceful degradation on partial failures
- Save state to allow resuming after crashes

---

### 8. **TODO Comments Indicate Incomplete Features**
**File:** Multiple files

**Found TODOs:**
1. `workflows/metadata.py:93` - Check if posting already in database (duplicate comment)
2. `markdown.py:24` - Do not read XML or boil it down to a few lines
3. `markdown.py:46` - Test async extraction performance
4. `files.py:121` - Rename to *.labels.json format
5. `files.py:128` - Add proper way to handle label file format & naming
6. `queries.py:477-480` - Add checks for score, text, start, end position equality

**Impact:**
- Indicates features may not be fully implemented
- Potential for unexpected behavior

---

## Testing Results

### Environment Setup
✅ **PASSED** - Dependencies installed successfully with uv
✅ **PASSED** - CLI commands available and responsive
✅ **PASSED** - Database schema creation works

### Pipeline Testing
❌ **FAILED** - Discovery: Website returns 403 Forbidden
⏭️ **SKIPPED** - Fetch archives: No postings to test with
⏭️ **SKIPPED** - Extract markdown: No archives to test with
⏭️ **SKIPPED** - Export: No data to export

---

## Recommendations

### Immediate Actions
1. **Fix the 403 error** - This is blocking all functionality
   - Add proper User-Agent headers
   - Implement request throttling
   - Test from GitHub Actions environment
   - Consider using requests.Session() with proper headers

2. **Fix parent_id bug** - This breaks archive structure
   - Implement proper ID assignment after commit
   - Add test to verify file tree structure

3. **Fix CLI path handling** - Small but important bug
   - Use proper None checking instead of `or` with Path

### Long-term Improvements
1. Add comprehensive error handling and retry logic
2. Resolve naming conflicts (ArchiveContent)
3. Add integration tests with mock data
4. Document async usage patterns
5. Complete TODO items or remove them
6. Add logging for debugging website access issues
7. Consider implementing a caching layer for resilience

---

## Next Steps

To continue testing the pipeline:
1. Investigate 403 error - check if GitHub Actions can access the site
2. Create mock data to test remaining components
3. Add unit tests for critical functions
4. Test with sample ZIP archives
5. Verify export functionality with sample data

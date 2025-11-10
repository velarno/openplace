# LLM-based Entity Extraction

This document describes the new LLM-based entity extraction pipeline that replaces the previous GLiNER-based NER system.

## Overview

The extraction pipeline uses Pydantic AI agents with large language models (LLMs) to extract structured information from French public tender documents. This approach provides:

- **Better accuracy**: LLMs understand context and can handle complex document structures
- **Structured output**: Uses Pydantic models for type-safe, validated extractions
- **Flexibility**: Easy to add new entity types or modify extraction logic
- **Multiple providers**: Support for Claude, OpenAI, Azure OpenAI, and more

## Extracted Entities

The system extracts 5 types of information:

1. **Selection Criteria** (`critere_de_selection_ou_exigence`): Requirements, qualifications, or selection criteria that applicants must meet
2. **Project Duration** (`duree_du_projet`): Information about project timeline, duration, start/end dates
3. **Application Deadline** (`date_limite_candidature`): Deadlines for submitting applications or bids
4. **Deliverable Type** (`type_de_livrable`): What must be delivered (services, goods, construction, reports, etc.)
5. **Budget Amount** (`prix_ou_budget_prevu_en_euros`): Budget, estimated cost, or price information

## Usage

### Command Line

Basic usage:
```bash
# Extract from up to 10 unprocessed documents using Claude
openplace extract-entities --limit 10

# Use OpenAI GPT-4
openplace extract-entities --limit 5 --model gpt-4o

# Use Azure OpenAI with 2 concurrent extractions
openplace extract-entities --limit 20 --model azure:gpt-4-deployment --concurrent 2
```

### Configuration

#### Claude (Anthropic)
```bash
export ANTHROPIC_API_KEY='sk-ant-...'
openplace extract-entities --model claude-3-5-sonnet-20241022
```

#### OpenAI
```bash
export OPENAI_API_KEY='sk-...'
openplace extract-entities --model gpt-4o
```

#### Azure OpenAI
```bash
export AZURE_OPENAI_API_KEY='...'
export AZURE_OPENAI_ENDPOINT='https://your-resource.openai.azure.com'
openplace extract-entities --model azure:your-deployment-name
```

### Options

- `--limit, -l`: Maximum number of documents to process (default: 10)
- `--model, -m`: Model to use (default: `claude-3-5-sonnet-20241022`)
- `--concurrent, -c`: Number of concurrent extractions (default: 1)
- `--api-key, -k`: API key (optional, uses environment variables by default)
- `--debug, -D`: Enable debug logging

### Rate Limits

Be careful with the `--concurrent` option:
- Start with 1 concurrent extraction
- Monitor your API rate limits
- Claude Sonnet typically allows 5 requests/second
- Azure OpenAI limits vary by deployment

## Architecture

### Components

1. **Pydantic Models** (`openplace/tasks/extract/llm.py`):
   - `TenderExtraction`: Main output model with all entity types
   - `EntitySpan`: Base model for extracted entities with text and position
   - Specific models for each entity type

2. **Agent** (`create_extraction_agent`):
   - Uses Pydantic AI framework
   - Configured with extraction-specific system prompt
   - Supports multiple LLM providers

3. **Workflow** (`openplace/workflows/extraction.py`):
   - `extract_and_store_labels`: Extracts from single document and stores in DB
   - `batch_extract_labels`: Processes multiple documents with concurrency control
   - `run_batch_extraction`: Synchronous wrapper for CLI

### Data Flow

```
Unprocessed ArchiveContent
         ↓
LLM Agent (Pydantic AI)
         ↓
TenderExtraction (structured)
         ↓
Convert to label format
         ↓
Store in ArchiveLabel table
         ↓
Mark content as processed
```

## Output Format

The extraction results are stored in the `ArchiveLabel` table with the following schema:

```python
{
    "archive_id": int,        # FK to ArchiveEntry
    "label": str,             # Entity type (e.g., "date_limite_candidature")
    "text": str,              # Extracted text
    "start": int,             # Start character position
    "stop": int,              # End character position
    "score": float,           # Confidence (1.0 for LLM extractions)
}
```

This format is compatible with the previous GLiNER pipeline, so existing queries and exports continue to work.

## Testing

A test script is provided to verify the extraction pipeline:

```bash
export ANTHROPIC_API_KEY='sk-ant-...'
python test_extraction.py
```

This runs the extraction on a sample French tender document and displays the results.

## Workflow Integration

### Updated GitHub Actions

The `.github/workflows/entity-recognition.yml` workflow has been updated to use the new extraction command:

```yaml
- name: Run LLM extraction
  env:
    ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
  run: |
    uv venv
    source .venv/bin/activate
    uv sync
    openplace extract-entities --limit 100 --concurrent 2
```

### Local Development

For local testing:

```bash
# Install dependencies
uv sync

# Run extraction on a few documents
openplace extract-entities --limit 5 --debug
```

## Comparison with GLiNER

| Feature | GLiNER (old) | LLM-based (new) |
|---------|--------------|-----------------|
| Accuracy | Moderate | High |
| Context understanding | Limited | Excellent |
| Setup complexity | High (model download, dependencies) | Low (API key only) |
| Speed | Fast (local) | Moderate (API calls) |
| Cost | Free (local) | Pay per use |
| Maintenance | Broken, hard to fix | Simple, well-supported |
| Customization | Difficult | Easy (change prompt) |
| Multiple entity types | Separate calls | Single call |

## Troubleshooting

### "No unprocessed archive contents found"

This means all archive contents in the database have already been processed. To reprocess:

```python
from openplace.storage.local import queries as q
# Reset inference flag for specific archive
q.set_archive_content_inference_done(archive_id, done=False)
```

### API Rate Limit Errors

Reduce concurrency:
```bash
openplace extract-entities --concurrent 1 --limit 5
```

### Out of Memory

Process in smaller batches:
```bash
openplace extract-entities --limit 10
```

### Azure OpenAI Authentication Errors

Ensure both environment variables are set:
```bash
export AZURE_OPENAI_API_KEY='your-key'
export AZURE_OPENAI_ENDPOINT='https://your-resource.openai.azure.com'
```

## Future Improvements

- [ ] Add retry logic for transient API errors
- [ ] Support for custom entity types via config file
- [ ] Validation and normalization of extracted dates/amounts
- [ ] Extraction confidence scoring from LLM reasoning
- [ ] Support for document-level metadata extraction
- [ ] Integration with evaluation framework for accuracy tracking

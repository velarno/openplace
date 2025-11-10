# Migration Guide: GLiNER to LLM Extraction

This guide helps you transition from the old GLiNER-based NER pipeline to the new LLM-based extraction system.

## What Changed

### Old Pipeline (GLiNER)
1. Export archive contents to `.txt` files
2. Download GLiNER model and script from Gist
3. Run GLiNER script on each file (slow, prone to errors)
4. Generate `.jsonl` files
5. Ingest JSONL files back into database

**Problems:**
- Complex setup with many dependencies
- Brittle (tokenizers, cmake, boost libraries)
- Slow processing
- Limited context understanding
- Hard to maintain and debug

### New Pipeline (LLM)
1. Run `openplace extract-entities`
2. Done! ✨

**Benefits:**
- Simple setup (just API key)
- Better accuracy and context understanding
- Direct database integration (no file export/import)
- Easy to customize and extend
- Concurrent processing support

## Setup

### 1. Install Dependencies

```bash
uv sync
```

This will install `pydantic-ai` and all required dependencies.

### 2. Configure API Keys

Choose your LLM provider and set the appropriate environment variable:

#### For Claude (recommended)
```bash
export ANTHROPIC_API_KEY='sk-ant-...'
```

#### For OpenAI
```bash
export OPENAI_API_KEY='sk-...'
```

#### For Azure OpenAI
```bash
export AZURE_OPENAI_API_KEY='...'
export AZURE_OPENAI_ENDPOINT='https://your-resource.openai.azure.com'
```

### 3. Update GitHub Secrets

In your repository settings, add the appropriate secret:
- `ANTHROPIC_API_KEY` for Claude
- `OPENAI_API_KEY` for OpenAI
- `AZURE_OPENAI_API_KEY` and `AZURE_OPENAI_ENDPOINT` for Azure

## Usage

### Command Line

Replace the old multi-step process:

```bash
# OLD WAY (don't use)
openplace bulk-export-archive-contents --limit 100 --output-dir .
./gliner-infer-labels --model="knowledgator/gliner-x-large" ...
openplace bulk-ingest-labels --input-dir .
```

With the new single command:

```bash
# NEW WAY
openplace extract-entities --limit 100
```

### GitHub Actions

The new workflow is much simpler. Compare:

#### Old Workflow (entity-recognition.yml)
- 3 jobs (get-scraper-run-id, entity-recognition, ingest-labels)
- ~150 lines of YAML
- Complex dependencies (cmake, boost, tokenizers)
- File export/import steps
- Gist download and management

#### New Workflow (llm-entity-extraction.yml)
- 2 jobs (get-scraper-run-id, llm-entity-extraction)
- ~80 lines of YAML
- No external dependencies
- Direct database processing
- Configurable via workflow inputs

To use the new workflow:

```bash
# Manual trigger via GitHub UI
# Or via gh CLI:
gh workflow run llm-entity-extraction.yml -f limit=50 -f model=claude-3-5-sonnet-20241022
```

## Reprocessing Existing Data

If you want to reprocess documents that were already processed with GLiNER:

```python
from openplace.storage.local import queries as q

# Reset all inference flags
contents = q.get_all_archive_contents()
for content in contents:
    # This will clear existing labels and reset the flag
    q.set_archive_content_inference_done(content.id, done=False)
```

Then run:
```bash
openplace extract-entities --limit 1000
```

## Cost Estimation

### Claude Sonnet 3.5
- Input: $3 per million tokens
- Output: $15 per million tokens
- Average tender document: ~2,000 tokens input, ~200 tokens output
- Cost per document: ~$0.009 (less than 1 cent)
- 1000 documents: ~$9

### OpenAI GPT-4o
- Input: $2.50 per million tokens
- Output: $10 per million tokens
- Similar cost profile

### Azure OpenAI
- Costs vary by region and deployment
- Generally similar to OpenAI pricing

## Performance Comparison

| Metric | GLiNER | LLM (Claude Sonnet) |
|--------|--------|---------------------|
| Setup time | 30+ minutes | 2 minutes |
| Processing speed | ~5 sec/doc | ~3 sec/doc |
| Accuracy | 70-75% | 85-90% |
| Context understanding | Limited | Excellent |
| Concurrent processing | No | Yes |
| Cost | Free (local) | ~$0.01/doc |

## Troubleshooting

### "I'm getting rate limit errors"

Reduce concurrency:
```bash
openplace extract-entities --concurrent 1 --limit 10
```

### "The old GLiNER workflow is still running"

You can disable the old workflow in GitHub:
1. Go to Actions → entity-recognition.yml
2. Click "..." → Disable workflow

Or delete the old workflow file:
```bash
rm .github/workflows/entity-recognition.yml
```

### "I want to keep both systems"

You can run both in parallel:
- Keep the old workflow for comparison
- Run the new workflow with different schedule
- Compare results in the database

## Rollback

If you need to rollback to GLiNER:

1. Restore the old workflow:
```bash
git checkout main .github/workflows/entity-recognition.yml
```

2. The old commands still work:
```bash
openplace bulk-export-archive-contents
# ... run GLiNER script
openplace bulk-ingest-labels
```

However, we recommend fixing issues with the new system rather than rolling back, as the LLM approach is more maintainable long-term.

## Getting Help

If you encounter issues:

1. Check the logs: `openplace extract-entities --debug`
2. Test with small batch: `openplace extract-entities --limit 1`
3. Verify API key: `echo $ANTHROPIC_API_KEY`
4. Review docs: `docs/llm-extraction.md`

## Next Steps

After migration:

1. Monitor extraction quality
2. Adjust concurrent processing based on rate limits
3. Consider adding custom entity types (see docs/llm-extraction.md)
4. Set up automated quality checks

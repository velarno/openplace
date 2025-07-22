# Remaining features to add

This file tracks ongoing or future features.

## Future Features

### Workflows

Features related to data retrieval/formatting through github actions / runners / remote CRON jobs.

- [ ] Add step to dump found pages at start, and only refetch those not seen in gh actions
- [ ] Add env var to gh action to specify number of pages to fetch, currently default is 1

### Content processing

Features related to PLACE files and their content, especially data cleaning / data transformation workflows.

- [ ] Add feature & step to process raw file contents and extract:
  - [ ] Field/Kind of service (e.g. IT/Construction/Logistics/Accounting/...)
  - [ ] Total contract value (e.g. replacing the entire town's water pipes for XYZ billion euros)
  - [ ] Deadline or estimated due date (e.g. proposals to be sent before 04/10/2026)
- [ ] Add custom writers for s3/blob remote archive storage (as of now only local FS supported)
  - [ ] Optional S3 plugin for GCP & AWS
  - [ ] Optional Azure plugin for Azure
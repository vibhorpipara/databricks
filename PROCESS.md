# PROCESS.md

# BLS Productivity & US Population Data Engineering Project

## 1. Project Overview

This project implements an end-to-end data engineering solution for ingesting U.S. Bureau of Labor Statistics (BLS) productivity data and U.S. population data, transforming the data through Bronze, Silver, and Gold layers, and exposing business-ready analytical results.

The solution is implemented in Databricks using Unity Catalog, Unity Catalog Volumes, Spark Declarative Pipelines (SDP), Spark SQL, and PySpark.

### Key objectives

- Dynamically discover and ingest the complete contents of the BLS productivity time-series directory.
- Store source files in a Unity Catalog Volume.
- Handle BLS source changes safely across repeated runs.
- Detect newly added, changed, and removed source files.
- Ingest the DataUSA population API response as JSON.
- Build Bronze, Silver, and Gold data layers.
- Apply data-quality expectations.
- Answer three required analytical questions.
- Implement each analytical question in both Spark SQL and PySpark.
- Provide a simple Genie interface and dashboard as bonus deliverables.

---

## 2. Architecture

```text
BLS Directory                 DataUSA Population API
      |                                |
      |                                |
      +------------ Source Ingestion --+
                       |
                       v
             Unity Catalog Volume
             /Volumes/bls/bronze/
                   bls_raw/
                     |
                     v
                  Bronze
                     |
                     v
                  Silver
                     |
                     v
                   Gold
                     |
              +------+------+
              |             |
              v             v
          Dashboard       Genie
```

### Unity Catalog objects

```text
Catalog:
  bls

Schemas:
  bls.bronze
  bls.silver
  bls.gold

Volume:
  bls.bronze.bls_raw
```

Raw landing location:

```text
/Volumes/bls/bronze/bls_raw/raw/
```

Temporary download location:

```text
/Volumes/bls/bronze/bls_raw/tmp/
```

The source acquisition and control logic runs before the SDP pipeline. The SDP pipeline starts from the raw files and builds the Bronze, Silver, and Gold datasets. The ingestion control table is stored as a Delta table, and the SDP-managed materialized views are Delta-backed managed datasets.

---

## 3. Source Ingestion

### 3.1 BLS source

The BLS productivity directory is:

```text
https://download.bls.gov/pub/time.series/pr/
```

The ingestion does not hardcode the BLS filenames.

The directory HTML is downloaded and parsed dynamically. Each discovered file is captured with:

- file name
- source URL
- source last-modified timestamp
- source file size

This allows the ingestion process to adapt if files are added or removed from the source directory.

### 3.2 BLS User-Agent

The BLS endpoint returned HTTP 403 when accessed without an appropriate User-Agent.

The ingestion therefore uses a descriptive User-Agent containing project identification and contact information, as expected by the BLS website.

This was required for successful source access.

### 3.3 Download validation

For each file selected for ingestion:

1. Download the source file.
2. Compare the downloaded byte size with the size advertised by the BLS directory.
3. Write the file to the temporary directory.
4. Validate the temporary file size.
5. Remove the existing raw file if present.
6. Move the validated temporary file into the raw directory.
7. Update the control table.

The temporary-to-raw movement prevents a partially downloaded file from replacing the existing raw file.

---

## 4. Safe Reruns and Change Detection

The control table is:

```text
bls.bronze.bls_file_control
```

Its purpose is to maintain the current ingestion state of each BLS source file.

### Control table columns

```text
file_name
source_url
source_last_modified
source_size
volume_path
ingestion_status
first_ingested_at
last_ingested_at
```

### Change detection

The current source metadata is compared with the control table.

A file is ingested when:

- it has never been seen before;
- it was previously marked `SOURCE_REMOVED` and has reappeared; or
- its source last-modified timestamp changed; or
- its source size changed.

If neither the timestamp nor size has changed, the file is considered unchanged and is not downloaded again.

### Removed files

If a file exists in the control table but is no longer discovered in the BLS directory:

```text
ingestion_status = SOURCE_REMOVED
```

The raw file is intentionally retained in the Volume for audit/history purposes. The source-ingestion control state prevents it from being downloaded again while it remains absent from the source.

If the same file later reappears at the source, it is detected and ingested again.

### Why a control table is used

The control table provides persistent ingestion state across notebook executions and avoids relying only on the current contents of the raw directory.

The implementation uses `MERGE` so that each source file has one current-state control record instead of accumulating duplicate control records across reruns.

### Idempotency

For this assignment, the combination of:

```text
source_last_modified + source_size
```

is used for change detection.

This is appropriate for the source and assignment requirements, but it is not a cryptographic guarantee that file content is unchanged.

In a production implementation, additional source-provided identifiers such as ETag/version information or a content hash could be considered where available.

---

## 5. DataUSA Population Ingestion

The population source is the DataUSA API:

```text
https://honolulu-api.datausa.io/tesseract/data.jsonrecords
```

The API response is stored as:

```text
/Volumes/bls/bronze/bls_raw/raw/datausa/population.json
```

The complete API response is retained as JSON rather than storing only the `data` array.

The JSON contains metadata and population records. The Bronze layer extracts the nested `data` array into tabular records.

A separate control table was not introduced for DataUSA because the assignment only requires the population response to be saved as JSON in the same Volume.

---

## 6. Bronze Layer

The Bronze layer represents the source data in structured form with minimal transformation.

### Main Bronze datasets

```text
bronze_bls_all_data
bronze_bls_series
bronze_bls_sector
bronze_bls_class
bronze_bls_measure
bronze_bls_duration
bronze_bls_seasonal
bronze_population
```

### BLS data

`pr.data.1.AllData` is loaded using an explicit Spark schema:

```text
series_id
year
period
value
footnote_codes
```

The BLS metadata files are loaded separately so that the series can later be enriched with human-readable descriptions.

### Population

The nested DataUSA JSON is parsed using an explicit schema and the `data` array is exploded into rows.

The resulting population dataset contains:

```text
Nation_ID
Nation
year
population
```

### Data-quality expectations

The SDP Bronze datasets use expectations for required fields such as:

- `series_id IS NOT NULL`
- `year IS NOT NULL`
- `period IS NOT NULL`
- `value IS NOT NULL`
- population `year IS NOT NULL`
- population `population IS NOT NULL`

Explicit Spark schemas provide expected data types, while SDP expectations validate record-level data quality.

---

## 7. Silver Layer

The Silver layer cleans and enriches the Bronze data.

### 7.1 Population

The population data is cleaned and duplicate records are removed based on:

```text
Nation_ID + year
```

The resulting dataset is:

```text
bls.silver.silver_population
```

### 7.2 BLS enrichment

The BLS data is enriched by joining:

```text
bronze_bls_all_data
       |
       +-- bronze_bls_series
       +-- bronze_bls_sector
       +-- bronze_bls_class
       +-- bronze_bls_measure
       +-- bronze_bls_duration
       +-- bronze_bls_seasonal
```

The resulting dataset is:

```text
bls.silver.silver_bls_data
```

The Silver dataset contains both BLS codes and human-readable metadata.

A `series_label` is constructed from the relevant descriptive fields, including sector, class, measure, duration, and seasonal information.

This makes the Gold output understandable without requiring users to know the BLS coding system.

### 7.3 Series ID normalization

During validation, the BLS source was found to contain trailing spaces in some `series_id` values.

For example, the raw value for:

```text
PRS30006032
```

contained trailing spaces.

The cleanup is performed in Silver using `trim()` on `series_id`.

This keeps Bronze close to the original source while making Silver suitable for joins, filtering, and analytical use.

---

## 8. Gold Layer

The Gold layer contains business-oriented datasets answering the three required analytical questions.

### Gold datasets

```text
bls.gold.gold_population_statistics
bls.gold.gold_bls_best_year
bls.gold.gold_series_population
```

---

## 9. Analytical Question 1

### Requirement

Calculate the mean and standard deviation of annual U.S. population from 2013 through 2018 inclusive.

### Gold table

```text
bls.gold.gold_population_statistics
```

### Primary implementation

Spark SQL is used as the primary implementation.

The calculation uses:

```text
AVG(population)
STDDEV_POP(population)
```

`STDDEV_POP` is used because the calculation treats the six annual population values from 2013–2018 as the complete population of observations being analyzed, rather than as a sample from a larger set.

The Gold table retains the calculated values without rounding so that downstream consumers can apply their own display formatting.

### Validation

The calculated mean population for 2013–2018 is approximately:

```text
322,069,808
```

The PySpark equivalent using `avg()` and `stddev_pop()` was also implemented as the alternative approach.

---

## 10. Analytical Question 2

### Requirement

For every `series_id` in the BLS data:

- calculate the annual sum of `value` across quarters;
- identify the year with the largest annual sum;
- provide a human-readable description of the series.

### Gold table

```text
bls.gold.gold_bls_best_year
```

### Logic

The Silver data is first aggregated by:

```text
series_id
series_label
year
```

The quarterly values are summed to produce an annual value.

A window function then ranks the years within each series by:

```text
annual_value DESC
year DESC
```

The first row for each series is selected.

The `year DESC` secondary ordering provides deterministic behavior if two years have exactly the same annual value.

### Output

The Gold table contains:

```text
series_id
series_label
best_year
annual_value
```

The `best_year` field is the year in which the largest annual sum occurs.

The human-readable `series_label` is included so users do not need to interpret BLS codes manually.

### Validation

The resulting dataset contains one record for each distinct BLS series.

The implementation was validated against the distinct series count in the source data.

### Alternative implementation

The same calculation was implemented in PySpark using:

- `groupBy`
- `sum`
- `Window.partitionBy`
- `row_number`

Spark SQL remains the primary implementation.

---

## 11. Analytical Question 3

### Requirement

For:

```text
series_id = PRS30006032
period = Q01
```

return the value for each year and join it with the population for that year where population data is available.

### Gold table

```text
bls.gold.gold_series_population
```

### Join

A `LEFT JOIN` is used between BLS and population data on:

```text
year
```

The left join is intentional because BLS data should remain available even when population data is missing for a particular year.

For example, if population is unavailable for a year, the population field remains NULL rather than removing the BLS record.

### Output

The Gold table contains:

```text
year
series_id
series_label
period
value
population
```

The result was validated after normalizing the trailing spaces in `series_id`.

### Alternative implementation

The same logic was implemented in PySpark using DataFrame filtering and a left join.

---

## 12. Spark SQL vs PySpark

Each analytical question was implemented using both Spark SQL and PySpark.

### Spark SQL

Spark SQL was selected as the primary implementation because the Gold questions are analytical and naturally expressed using:

- aggregations;
- joins;
- common table expressions;
- window functions.

It is also easy for SQL-oriented analysts to understand and review.

### PySpark

PySpark implementations are retained as alternatives to demonstrate equivalent DataFrame-based transformations and provide flexibility for future logic that may be easier to express programmatically.

Only one implementation feeds each Gold dataset to avoid maintaining two separate production outputs for the same business logic.

---

## 13. Data Quality and Validation

Validation was performed at multiple stages.

### Bronze validation

Checks included:

- row counts;
- schema;
- null checks;
- distinct series counts;
- successful loading of metadata tables.

### Silver validation

Checks included:

- Silver row count compared with Bronze;
- duplicate `series_id + year + period` checks;
- required field null checks;
- metadata enrichment checks;
- verification that BLS series IDs match expected values after trimming.

### Gold validation

Checks included:

- population statistics for 2013–2018;
- one best-year record per BLS series;
- Q01 values for `PRS30006032`;
- population join behavior for years with and without population data.

The final pipeline outputs were validated against the assignment requirements.

---

## 14. Performance and Cost Considerations

The source files are relatively small for this assignment, but the BLS `pr.data.1.AllData` file contains a significant number of rows compared with the metadata files.

The SDP pipeline uses materialized views for the required Bronze, Silver, and Gold datasets. The current pipeline is run with a Refresh all operation because the assignment dataset is relatively small; the incremental behavior is implemented at the source-ingestion layer rather than by claiming the SDP pipeline itself is incremental.

Because these datasets are materialized views and data-quality expectations are applied, repeated Refresh all operations can involve substantial recomputation. This was observed particularly for the large BLS data dataset.

For the assignment, correctness and clear implementation were prioritized over aggressive optimization.

For a larger production workload, possible improvements could include:

- incremental processing where supported by the pipeline architecture;
- partitioning or clustering strategies based on actual query patterns;
- avoiding unnecessary full refreshes;
- isolating expensive transformations;
- monitoring execution duration and compute consumption;
- selecting appropriate table types for incremental workloads.

---

## 15. Schema Drift

The BLS directory is dynamically discovered, which means new files can be detected without changing the ingestion code.

However, dynamic file discovery does not automatically mean that a newly added file has a known schema.

For production use, schema handling should include:

- schema validation;
- schema versioning;
- controlled schema evolution;
- alerts when expected columns or data types change.

For this assignment, explicit schemas are used for the known BLS files required by the analytical questions.

---

## 16. Error Handling

The ingestion process validates source responses and downloaded file sizes before replacing raw files.

Examples of handled conditions include:

- HTTP request failures;
- BLS 403 access requirements;
- source size mismatch;
- temporary file validation failure;
- missing expected DataUSA JSON structure.

The temporary landing approach also reduces the risk of replacing a valid raw file with an incomplete download.

For a production implementation, additional operational handling could include:

- retry policies;
- alerting;
- centralized logging;
- dead-letter/error locations;
- orchestration-level failure notifications.

---

## 17. Access Control

Unity Catalog is used as the governance layer for catalogs, schemas, tables, and volumes.

A production deployment would provide analysts with read-only access to the Gold schema while restricting write access to engineering/service principals.

The Free Edition environment did not provide a suitable separate analyst principal for a meaningful end-to-end demonstration of this access-control scenario, so this was documented rather than artificially implemented.

---

## 18. Monitoring

The current solution provides ingestion state through:

```text
bls.bronze.bls_file_control
```

Useful production monitoring metrics would include:

- files discovered;
- files newly ingested;
- files changed;
- files removed;
- ingestion failures;
- ingestion duration;
- row counts;
- data-quality expectation failures;
- pipeline execution duration;
- compute consumption.

These metrics could be connected to operational alerting in a production environment. The current project does not configure production alerts; monitoring and alerting are documented as production considerations.

---

## 19. Bonus Deliverables

### 19.1 Genie

A Databricks Genie Agent was configured using the Gold tables.

Instructions guide Genie to:

- use Gold tables as the primary source;
- use `series_label` for human-readable BLS descriptions;
- use `gold_bls_best_year` for best-year questions;
- use `gold_series_population` for `PRS30006032` Q01 questions;
- use `gold_population_statistics` for population statistics;
- clearly indicate when population data is unavailable.

The Genie implementation was validated using a population question and returned the same mean population calculated in the Gold table.

### 19.2 Dashboard

A Databricks dashboard was created using the Gold tables.

The dashboard contains visualizations for:

1. Mean population.
2. Population standard deviation.
3. BLS best year by series.
4. `PRS30006032` Q01 annual trend.

The dashboard provides a simple business-facing view of the analytical outputs without exposing the underlying ingestion complexity.

---

## 20. Deployment and Repository Structure

The project is organized into notebooks, pipeline definitions, dashboard assets, screenshots, and documentation.

Repository structure:

```text
BLS/
├── dashboard/
├── notebooks/
├── pipeline/
├── screenshots/
├── PROCESS.md
└── README.md
```

The repository contains the source code required to reproduce the project, supporting dashboard/screenshot assets, and the documentation needed to understand the design decisions.

---

## 21. Key Design Decisions

| Decision | Reason |
|---|---|
| Dynamic BLS file discovery | Avoid hardcoded filenames and detect source changes |
| Control table | Persist ingestion state between runs |
| `source_last_modified + source_size` | Simple source change detection suitable for this assignment |
| Temporary download location | Prevent incomplete downloads from replacing valid raw files |
| Retain removed raw files | Preserve audit/history |
| Explicit Spark schemas | Control expected data types |
| Bronze/Silver/Gold layers | Separate ingestion, enrichment, and business logic |
| Trim `series_id` in Silver | Preserve raw source while normalizing analytical keys |
| Human-readable `series_label` | Make BLS outputs understandable |
| Spark SQL as primary Gold implementation | Natural fit for analytical transformations |
| PySpark as alternative | Demonstrate equivalent DataFrame implementation |
| `STDDEV_POP` for Q1 | Treat 2013–2018 observations as the complete population being analyzed |
| LEFT JOIN for Q3 | Preserve BLS records when population is unavailable |
| No final ORDER BY in Gold tables | Ordering is applied only when presenting query results |
| Genie on Gold tables | Provide a natural-language business interface |
| Dashboard on Gold tables | Provide simple business-facing analytics |

---

## 22. Retrospective

Several practical issues were identified and resolved during implementation.

### BLS access restriction

The BLS directory initially returned HTTP 403. Research into the source requirements led to the use of a descriptive User-Agent with contact information.

### Dynamic directory parsing

The BLS directory uses a simple HTML/preformatted structure rather than a conventional HTML table. The metadata was therefore extracted dynamically from the directory response.

### Control table duplicates

An early version of the ingestion logic created duplicate control records during repeated testing. The implementation was changed to use `MERGE`, providing one current-state record per source file.

### DataUSA nested JSON

The population API returns records inside a top-level `data` array. The Bronze ingestion was updated to explicitly model the nested structure and explode the array into rows.

### BLS trailing spaces

The BLS `series_id` field contains fixed-width values with trailing spaces. This caused the initial Q3 filter for `PRS30006032` to return no rows.

The issue was resolved by trimming the key in Silver rather than modifying the raw Bronze data.

### SDP target schemas

The project initially used the `schema` parameter of `@dp.materialized_view` incorrectly. In SDP, that parameter describes the dataset's column schema rather than the Unity Catalog target schema.

Fully qualified dataset names such as:

```text
bls.silver.silver_population
bls.gold.gold_population_statistics
```

were therefore used to place datasets in the intended Unity Catalog schemas.

---

## 23. AI Usage

AI was used during the development of this project for:

- assisting with selected portions of code and code refinement;
- troubleshooting implementation issues;
- evaluating design alternatives;
- improving project documentation.

---

## 24. Final Outcome

The completed solution provides:

- Dynamic BLS source discovery.
- Full BLS raw-file ingestion.
- DataUSA population ingestion.
- Safe rerun/change detection.
- Added/changed/removed source-file handling.
- Persistent ingestion control state.
- Bronze, Silver, and Gold data layers.
- Data-quality expectations.
- Human-readable BLS metadata enrichment.
- Three required analytical outputs.
- Spark SQL and PySpark implementations.
- Validation of the final outputs.
- Databricks Genie bonus.
- Databricks dashboard bonus.

The resulting architecture separates source acquisition and ingestion control from analytical processing, while keeping the Gold layer focused on the business questions required by the assignment.

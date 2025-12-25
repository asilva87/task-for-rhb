# Train Delay Analytics – Minimal ELT Pipeline

> **Context**  
> This small project was created to show my interest in the position of  
> **Data Engineer (50–100%) at RhB**  
> (https://www.rhb.ch/de/job/data-engineer-50-100_2025-2097/).  
>  
> It is intentionally lightweight and local, and focuses on clear structure,
> reasonable data-quality decisions, and explainable results rather than on
> tooling or cloud infrastructure.

## Purpose

This project shows a **small, local ELT pipeline** built on railway delay data, using a **Bronze–Silver–Gold (Medallion) structure**.  
The goal is simply to demonstrate ingesting, cleaning, and preparing data for analysis.

The dataset contains train stop events with planned and actual times, delays, station information and basic classifications.

---

## Data Layers (Medallion Structure)

### Bronze – Raw Data

- Source: `DBtrainrides.csv`
- Stored as **Parquet** without any transformations
- Purpose:
  - Keep the raw data unchanged
  - Make reprocessing and debugging possible

**File**

    data/bronze/trainrides_bronze.parquet

---

### Silver – Cleaned & Structured Data

The Silver layer applies **simple data-quality rules**, while keeping the data as complete as possible.

What happens in this layer:
- Timestamp columns are parsed into proper datetime types
- Basic domain constraints are enforced (e.g. station category range)
- Obvious duplicates are removed
- Geographic coordinates are validated
- Station name is enforced as a required field (no rows were missing in this dataset)
- A boolean `is_delayed` column is added

**Delay definition**

- A delay is defined as **arrival delay > 6 minutes**
- This follows the delay classification already present in the dataset

**Purpose**

- Make assumptions explicit
- Produce data that is easy and safe to work with in analytics

**File**

    data/silver/trainrides_silver.parquet

---

### Gold – Analytics-Ready Output

The Gold layer contains a **simple business-facing aggregation** derived from the Silver data.

**Aggregation**
- Delay rate by station category

**Metrics**
- `total_events`
- `delayed_events`
- `delay_rate`

**File**

    data/gold/delay_rate_by_category.parquet

**Example result**

| category | total_events | delayed_events | delay_rate |
|---------:|-------------:|---------------:|-----------:|
| 1 | 69,656 | 3,351 | 4.81% |
| 2 | 133,827 | 6,425 | 4.80% |
| 5 | 635,943 | 29,598 | 4.65% |
| 3 | 412,783 | 18,859 | 4.57% |
| 4 | 777,685 | 26,855 | 3.45% |

---

## ELT Pipeline Overview

    CSV
      v
    Bronze (raw Parquet)
      v
    Silver (cleaned, typed, explicit rules)
      v
    Gold (aggregated analytics output)

The pipeline is implemented using small, repeatable Python scripts:

- `csv_to_parquet.py`
- `bronze_to_silver.py`
- `silver_to_gold.py`

---

## Notes

- This project is intentionally **lightweight** and **local** (no cloud)
- The focus is on:
  - clear structure
  - reasonable data-quality decisions
  - results that are easy to explain

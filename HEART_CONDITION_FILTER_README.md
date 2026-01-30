# Heart Condition Patient Filter -- End-to-End Guide

This document describes how to go from an empty output folder to a folder
populated with FHIR patient data for every patient in the MIMIC-IV demo
dataset who has a heart-related ICD diagnosis code.

## Prerequisites

1. **Python environment** -- Create and activate a virtual environment, then
   install dependencies:

   ```
   python -m venv fhir-server-venv
   fhir-server-venv\Scripts\activate      # Windows
   pip install -r requirements.txt
   ```

2. **MIMIC-IV FHIR demo data** -- Download version 2.1.0 from PhysioNet and
   extract it so the NDJSON files land at:

   ```
   mimic-iv-data/mimic-iv-clinical-database-demo-on-fhir-2.1.0/
       mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir/*.ndjson.gz
   ```

   Source: https://physionet.org/content/mimic-iv-fhir-demo/2.1.0/

3. **MIMIC-IV clinical notes** -- Download the discharge notes and place the
   gzipped CSV in the data directory:

   ```
   mimic-iv-data/discharge.csv.gz
   ```

   Source: https://physionet.org/content/mimic-iv-note/2.2/

4. **Patient mapping file** -- The file `fhir_patients_with_notes_mapping.json`
   must be present at the project root. It maps FHIR patient UUIDs to
   MIMIC subject IDs and identifies the 100 patients that have discharge
   notes.

## Step-by-Step Instructions

### Step 1 -- Start the FHIR server

In one terminal, start the server. It loads all FHIR resources and discharge
notes into memory:

```
python start_server.py
```

Wait until you see the "Running on http://..." message. The server must stay
running for the next step.

### Step 2 -- Run the heart condition filter

In a second terminal (with the same venv active), run:

```
python filter_by_icd.py
```

This will:

1. Scan the Condition NDJSON files for heart-related ICD codes.
2. Print every matching patient with their ICD codes.
3. Fetch each patient's full FHIR Bundle from the running server.
4. Save `_raw.json` and `_timeline.json` for each patient to the output
   directory.

Default output directory: `mimic-iv-data/patients/heart_conditions/`

### Optional flags

| Flag | Description |
|------|-------------|
| `--dry-run` | Identify matching patients and print them, but do not fetch or save any data. Does not require the server to be running. |
| `--output-dir PATH` | Override the output directory (default: `mimic-iv-data/patients/heart_conditions`). |
| `--base-url URL` | Override the FHIR server URL (default: `http://localhost:5000/fhir`). |
| `--fhir-data-path PATH` | Override the path to the NDJSON source files. |
| `--mapping-file PATH` | Override the patient mapping JSON file. |

Example dry run:

```
python filter_by_icd.py --dry-run
```

## ICD Code Ranges

The filter matches the following heart condition code ranges:

| System | Range | Description |
|--------|-------|-------------|
| ICD-10 | I20--I25 | Ischaemic heart diseases |
| ICD-10 | I30--I50 | Other forms of heart disease (pericarditis, endocarditis, cardiomyopathy, heart failure, etc.) |
| ICD-9 | 410--414 | Ischaemic heart disease |
| ICD-9 | 420--428 | Other forms of heart disease |

Matching is prefix-based, so code `I251` (atherosclerotic heart disease) matches
the `I25` prefix, and `4280` (congestive heart failure) matches the `428`
prefix.

Both `MimicCondition.ndjson.gz` and `MimicConditionED.ndjson.gz` are scanned.

## Output

After a successful run, the output directory contains two files per patient:

```
mimic-iv-data/patients/heart_conditions/
    {patient-uuid}_raw.json
    {patient-uuid}_timeline.json
```

### _raw.json

The complete FHIR Bundle returned by the server's `Patient/$everything`
endpoint. Contains every resource associated with the patient: Patient
demographics, Encounters, Conditions, Procedures, Observations,
MedicationRequests, and DocumentReferences (discharge notes).

### _timeline.json

A processed view of the raw bundle, organized by encounter:

- `demographics` -- patient birth date, gender, race, ethnicity, marital
  status, languages.
- `encounters[]` -- each encounter sorted chronologically, containing:
  - `events[]` -- dated clinical events (observations, procedures,
    medications, documents, conditions) that occurred during the encounter.
  - `undated_events[]` -- events linked to the encounter but lacking a
    timestamp.
- `non_encounter_events` -- dated events not linked to any encounter.
- `non_encounter_undated_events` -- undated events not linked to any encounter.
- `summary` -- counts of encounters, total events, dated vs. undated, and
  events with vs. without an encounter association.

## How It Works

The pipeline has two phases:

**Phase 1 -- Offline ICD scan** (`filter_by_icd.py` only, no server needed)

The script reads `MimicCondition.ndjson.gz` and `MimicConditionED.ndjson.gz`
directly, line by line. For each Condition resource it checks the
`code.coding[].code` and `code.coding[].system` fields against the heart
condition prefix lists. It collects the patient UUID (from `subject.reference`)
for every match, filtered to only patients present in the mapping file.

**Phase 2 -- Server fetch and timeline generation** (requires running server)

For each patient identified in Phase 1, the script uses `FhirClient` (from
`client_example.py`) to call the server's `Patient/{id}/$everything` endpoint.
The raw FHIR Bundle is saved as `_raw.json`. Then the client's three-pass
timeline processor organizes the bundle into encounter-grouped events and
saves the result as `_timeline.json`.

## File Reference

| File | Role |
|------|------|
| `start_server.py` | Launches the FHIR server with default paths |
| `mimic_fhir_server.py` | FHIR R4 server -- loads NDJSON data + discharge notes, serves REST API |
| `client_example.py` | FHIR client -- fetches patient data, builds timelines, saves JSON |
| `filter_by_icd.py` | Scans conditions for heart ICD codes, then uses the client to save matching patients |
| `fhir_patients_with_notes_mapping.json` | Maps patient UUIDs to subject IDs; lists patients with discharge notes |
| `requirements.txt` | Python dependencies (flask, pandas, fhir.resources, flask-cors, python-dateutil) |

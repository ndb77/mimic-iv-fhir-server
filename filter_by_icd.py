"""
Filter MIMIC-IV FHIR patients by ICD codes for heart conditions.

Identifies patients with heart-related ICD codes from the Condition NDJSON data,
then fetches their full FHIR data from the server and saves _raw.json and
_timeline.json to the heart_conditions output directory.

Heart condition ICD code ranges:
  ICD-10: I20-I25 (ischemic heart diseases)
  ICD-10: I30-I50 (other forms of heart disease)
  ICD-9:  410-414 (ischemic heart disease)
  ICD-9:  420-428 (other forms of heart disease)
"""

import json
import gzip
import argparse
import sys
from pathlib import Path

from client_example import FhirClient


# ICD-10 prefixes: I20-I25, I30-I50
ICD10_PREFIXES = (
    [f"I{n}" for n in range(20, 26)]
    + [f"I{n}" for n in range(30, 51)]
)

# ICD-9 prefixes: 410-414, 420-428
ICD9_PREFIXES = (
    [str(n) for n in range(410, 415)]
    + [str(n) for n in range(420, 429)]
)

ICD10_SYSTEM = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd10"
ICD9_SYSTEM = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd9"


def is_heart_condition_code(code, system):
    """Check if an ICD code falls within the heart condition ranges."""
    if not code:
        return False

    code_upper = code.upper()

    if system == ICD10_SYSTEM:
        return any(code_upper.startswith(prefix) for prefix in ICD10_PREFIXES)
    elif system == ICD9_SYSTEM:
        return any(code_upper.startswith(prefix) for prefix in ICD9_PREFIXES)

    return False


def find_heart_condition_patients(fhir_data_path, valid_patient_uuids=None):
    """
    Scan MimicCondition NDJSON files for patients with heart condition ICD codes.

    Returns a dict mapping patient_uuid -> list of matching ICD codes found.
    """
    condition_files = [
        fhir_data_path / "MimicCondition.ndjson.gz",
        fhir_data_path / "MimicConditionED.ndjson.gz",
    ]

    patient_codes = {}  # {patient_uuid: [{code, display, system}]}

    for condition_file in condition_files:
        if not condition_file.exists():
            print(f"  Skipping {condition_file.name} (not found)")
            continue

        print(f"  Scanning {condition_file.name}...")
        matched = 0

        with gzip.open(condition_file, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                resource = json.loads(line)

                # Extract patient UUID from subject reference
                subject_ref = resource.get("subject", {}).get("reference", "")
                if not subject_ref.startswith("Patient/"):
                    continue
                patient_uuid = subject_ref.split("/")[-1]

                # Skip patients not in the valid set
                if valid_patient_uuids and patient_uuid not in valid_patient_uuids:
                    continue

                # Check each coding entry for heart condition codes
                codings = resource.get("code", {}).get("coding", [])
                for coding in codings:
                    code = coding.get("code", "")
                    system = coding.get("system", "")
                    display = coding.get("display", "")

                    if is_heart_condition_code(code, system):
                        if patient_uuid not in patient_codes:
                            patient_codes[patient_uuid] = []
                        patient_codes[patient_uuid].append({
                            "code": code,
                            "display": display,
                            "system": system,
                        })
                        matched += 1

        print(f"    Found {matched} matching condition entries")

    return patient_codes


def load_valid_patient_uuids(mapping_file):
    """Load the set of valid patient UUIDs from the mapping file."""
    with open(mapping_file, "r") as f:
        mapping = json.load(f)

    uuids = set()
    uuid_to_subject = {}
    for patient in mapping["patients_with_notes"]:
        uuids.add(patient["patient_uuid"])
        uuid_to_subject[patient["patient_uuid"]] = patient["subject_id"]

    return uuids, uuid_to_subject


def main():
    parser = argparse.ArgumentParser(
        description="Filter MIMIC-IV patients by heart condition ICD codes"
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:5000/fhir",
        help="FHIR server base URL (default: http://localhost:5000/fhir)",
    )
    parser.add_argument(
        "--fhir-data-path",
        default="mimic-iv-data/mimic-iv-clinical-database-demo-on-fhir-2.1.0/"
                "mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir",
        help="Path to FHIR NDJSON data files",
    )
    parser.add_argument(
        "--mapping-file",
        default="fhir_patients_with_notes_mapping.json",
        help="Patient mapping JSON file",
    )
    parser.add_argument(
        "--output-dir",
        default="mimic-iv-data/patients/heart_conditions",
        help="Output directory for filtered patient data",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only identify patients, do not fetch/save data",
    )

    args = parser.parse_args()

    fhir_data_path = Path(args.fhir_data_path)
    if not fhir_data_path.exists():
        print(f"ERROR: FHIR data path not found: {fhir_data_path}")
        sys.exit(1)

    mapping_file = Path(args.mapping_file)
    if not mapping_file.exists():
        print(f"ERROR: Mapping file not found: {mapping_file}")
        sys.exit(1)

    # Load valid patient UUIDs
    print("Loading patient mapping...")
    valid_uuids, uuid_to_subject = load_valid_patient_uuids(mapping_file)
    print(f"  {len(valid_uuids)} patients with discharge notes")

    # Scan conditions for heart-related ICD codes
    print("\nScanning conditions for heart-related ICD codes...")
    patient_codes = find_heart_condition_patients(fhir_data_path, valid_uuids)

    print(f"\nFound {len(patient_codes)} patients with heart conditions")

    if not patient_codes:
        print("No matching patients found.")
        return

    # Print summary of matched patients and their codes
    print("\nMatched patients:")
    for patient_uuid, codes in sorted(patient_codes.items()):
        subject_id = uuid_to_subject.get(patient_uuid, "unknown")
        unique_codes = {c["code"] for c in codes}
        print(f"  subject_id={subject_id}  uuid={patient_uuid}")
        print(f"    codes: {', '.join(sorted(unique_codes))}")

    if args.dry_run:
        print("\n(dry run -- skipping data fetch)")
        return

    # Fetch and save data for each matching patient
    print(f"\nSaving patient data to {args.output_dir}...")
    client = FhirClient(args.base_url, args.mapping_file)

    success_count = 0
    fail_count = 0

    for i, patient_uuid in enumerate(sorted(patient_codes.keys()), 1):
        subject_id = uuid_to_subject.get(patient_uuid, "unknown")
        print(
            f"\n[{i}/{len(patient_codes)}] "
            f"Patient subject_id={subject_id} ({patient_uuid})"
        )

        try:
            result = client.save_patient_data(patient_uuid, args.output_dir)
            summary = result["timeline_summary"]
            print(
                f"  Resources: {result['total_resources']}, "
                f"Encounters: {summary.get('total_encounters', 0)}, "
                f"Events: {summary.get('total_events', 0)}"
            )
            success_count += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            fail_count += 1

    print(f"\nDone. Saved {success_count} patients, {fail_count} failures.")
    print(f"Output directory: {args.output_dir}")


if __name__ == "__main__":
    main()

"""
MIMIC-IV FHIR R4 Compliant Server

This server provides FHIR R4 compliant API access to MIMIC-IV demo data
filtered to only patients with discharge notes.

Endpoints:
- GET /fhir/metadata - Capability Statement
- GET /fhir/{ResourceType}/{id} - Read resource by ID
- GET /fhir/{ResourceType}?search_params - Search resources
- GET /fhir/Patient/{id}/$everything - Get all resources for a patient
"""

import json
import gzip
import logging
import argparse
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional
import uuid

from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)


class MimicFhirServer:
    """FHIR R4 compliant(?) server for MIMIC-IV data"""

    def __init__(self, fhir_data_path: str, notes_data_path: str, mapping_file: str = "fhir_patients_with_notes_mapping.json"):
        self.fhir_data_path = Path(fhir_data_path)
        self.notes_data_path = Path(notes_data_path)
        self.mapping_file = Path(mapping_file)

        # Storage
        self.resources = defaultdict(dict)  # {resource_type: {id: resource}}
        self.patient_map = {}  # {uuid: subject_id}
        self.subject_to_uuid = {}  # {subject_id: uuid}
        self.valid_patient_uuids = set()  # Only patients with notes

        logger.info("Initializing MIMIC-IV FHIR Server...")
        self.load_data()
        logger.info("MIMIC-IV FHIR Server initialized successfully")

    def load_data(self):
        """Load all FHIR data, discharge notes, and radiology notes"""
        # Load patient mapping first
        self._load_patient_mapping()

        # Load FHIR resources
        logger.info("Loading MIMIC-IV-FHIR-Demo data...")
        self._load_fhir_resources()

        # Load discharge notes and create DocumentReferences
        logger.info("Loading MIMIC-IV-Note data...")
        self._load_discharge_notes()

        # Load radiology notes and create DocumentReferences
        logger.info("Loading MIMIC-IV radiology notes...")
        self._load_radiology_notes()

        # Summary
        total_resources = sum(len(resources) for resources in self.resources.values())
        logger.info(f"Loaded {len(self.resources['Patient'])} patients")
        logger.info(f"Loaded {len(self.resources.get('Encounter', {}))} encounters")
        logger.info(f"Loaded {sum(len(r) for t, r in self.resources.items() if t.startswith('Observation'))} observations")
        logger.info(f"Loaded {len(self.resources.get('Condition', {}))} conditions")
        logger.info(f"Loaded {len(self.resources.get('Procedure', {}))} procedures")
        logger.info(f"Loaded {len(self.resources.get('MedicationRequest', {}))} medication requests")
        logger.info(f"Loaded {len(self.resources.get('DocumentReference', {}))} document references")
        logger.info(f"Total resources: {total_resources}")

    def _load_patient_mapping(self):
        """Load the mapping of patients with discharge notes"""
        logger.info(f"Loading patient mapping from {self.mapping_file}")
        with open(self.mapping_file, 'r') as f:
            mapping = json.load(f)

        # Extract valid patient UUIDs and create bidirectional mapping
        for patient in mapping['patients_with_notes']:
            uuid = patient['patient_uuid']
            subject_id = patient['subject_id']
            self.valid_patient_uuids.add(uuid)
            self.patient_map[uuid] = subject_id
            self.subject_to_uuid[subject_id] = uuid

        logger.info(f"Loaded {len(self.valid_patient_uuids)} patients with discharge notes")

    def _load_fhir_resources(self):
        """Load all FHIR resources from NDJSON files"""
        # Find all .ndjson.gz files
        fhir_files = list(self.fhir_data_path.glob("*.ndjson.gz"))

        for fhir_file in fhir_files:
            resource_type = fhir_file.stem.replace('.ndjson', '')
            logger.info(f"Loading {fhir_file.name}")

            with gzip.open(fhir_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        resource = json.loads(line)

                        # Filter: only include resources for patients with notes
                        if self._should_include_resource(resource):
                            resource_id = resource.get('id')
                            actual_type = resource.get('resourceType', resource_type)
                            self.resources[actual_type][resource_id] = resource

    def _should_include_resource(self, resource: dict) -> bool:
        """Check if a resource should be included (belongs to a patient with notes)"""
        resource_type = resource.get('resourceType')

        # Always include Organization and Location
        if resource_type in ['Organization', 'Location', 'Medication']:
            return True

        # For Patient, check if in valid set
        if resource_type == 'Patient':
            return resource.get('id') in self.valid_patient_uuids

        # For other resources, check subject/patient reference
        subject_ref = resource.get('subject', {}).get('reference', '')
        if subject_ref.startswith('Patient/'):
            patient_id = subject_ref.split('/')[-1]
            return patient_id in self.valid_patient_uuids

        # For Encounter, check subject
        if resource_type == 'Encounter':
            subject_ref = resource.get('subject', {}).get('reference', '')
            if subject_ref.startswith('Patient/'):
                patient_id = subject_ref.split('/')[-1]
                return patient_id in self.valid_patient_uuids

        # For resources without patient link, include them
        return True

    def _load_discharge_notes(self):
        """Load discharge notes and create DocumentReference resources"""
        discharge_file = self.notes_data_path / "discharge.csv.gz"

        if not discharge_file.exists():
            logger.warning(f"Discharge file not found: {discharge_file}")
            return

        logger.info(f"Loading {discharge_file.name}")

        # Read discharge notes
        df = pd.read_csv(discharge_file, compression='gzip')

        # Filter to only patients we have in FHIR
        df = df[df['subject_id'].astype(str).isin(self.subject_to_uuid.keys())]

        logger.info(f"Found {len(df)} discharge notes for patients with FHIR data")

        # Create DocumentReference for each note
        doc_refs_created = 0
        for _, row in df.iterrows():
            subject_id = str(row['subject_id'])
            patient_uuid = self.subject_to_uuid.get(subject_id)

            if patient_uuid:
                doc_ref = self._create_document_reference(row, patient_uuid)
                self.resources['DocumentReference'][doc_ref['id']] = doc_ref
                doc_refs_created += 1

        logger.info(f"Created {doc_refs_created} discharge DocumentReference resources")

    def _load_radiology_notes(self):
        """Load radiology notes and create DocumentReference resources"""
        radiology_file = self.notes_data_path / "radiology.csv.gz"

        if not radiology_file.exists():
            logger.warning(f"Radiology file not found: {radiology_file}")
            return

        logger.info(f"Loading {radiology_file.name}")

        # Read radiology notes
        df = pd.read_csv(radiology_file, compression='gzip')

        # Filter to only patients we have in FHIR
        df = df[df['subject_id'].astype(str).isin(self.subject_to_uuid.keys())]

        logger.info(f"Found {len(df)} radiology notes for patients with FHIR data")

        # Create DocumentReference for each note
        doc_refs_created = 0
        for _, row in df.iterrows():
            subject_id = str(row['subject_id'])
            patient_uuid = self.subject_to_uuid.get(subject_id)

            if patient_uuid:
                doc_ref = self._create_document_reference(row, patient_uuid)
                self.resources['DocumentReference'][doc_ref['id']] = doc_ref
                doc_refs_created += 1

        logger.info(f"Created {doc_refs_created} radiology DocumentReference resources")

    def _create_document_reference(self, note_row, patient_uuid: str) -> dict:
        """Create a FHIR DocumentReference from a clinical note"""
        note_id = note_row['note_id']
        hadm_id = str(note_row['hadm_id']) if pd.notna(note_row['hadm_id']) else None
        charttime = note_row['charttime']
        text = note_row['text']

        # Find the encounter if we have hadm_id
        encounter_ref = None
        if hadm_id:
            # Search for encounter with this hadm_id
            for enc_id, encounter in self.resources.get('Encounter', {}).items():
                for identifier in encounter.get('identifier', []):
                    if identifier.get('value') == hadm_id:
                        encounter_ref = f"Encounter/{enc_id}"
                        break
                if encounter_ref:
                    break

        # Create DocumentReference
        doc_ref_id = str(uuid.uuid4())

        # Parse charttime to ISO format
        try:
            dt = pd.to_datetime(charttime)
            date_str = dt.strftime('%Y-%m-%dT%H:%M:%S%z')
            if not date_str.endswith(('Z', '+', '-')):
                date_str += 'Z'
        except:
            date_str = str(charttime)

        # Select LOINC type code based on note_type
        note_type = note_row.get('note_type', 'DS')
        if note_type == 'RR':
            loinc_code = "18726-0"
            loinc_display = "Radiology studies"
        else:
            loinc_code = "18842-5"
            loinc_display = "Discharge summary"

        doc_ref = {
            "resourceType": "DocumentReference",
            "id": doc_ref_id,
            "identifier": [{
                "system": "http://mimic.mit.edu/fhir/mimic/identifier/note",
                "value": note_id
            }],
            "status": "current",
            "type": {
                "coding": [{
                    "system": "http://loinc.org",
                    "code": loinc_code,
                    "display": loinc_display
                }]
            },
            "category": [{
                "coding": [{
                    "system": "http://hl7.org/fhir/us/core/CodeSystem/us-core-documentreference-category",
                    "code": "clinical-note",
                    "display": "Clinical Note"
                }]
            }],
            "subject": {
                "reference": f"Patient/{patient_uuid}"
            },
            "date": date_str,
            "content": [{
                "attachment": {
                    "contentType": "text/plain",
                    "data": text  # Inline text
                }
            }]
        }

        if encounter_ref:
            doc_ref["context"] = {
                "encounter": [{"reference": encounter_ref}]
            }

        return doc_ref

    def get_resource(self, resource_type: str, resource_id: str) -> Optional[dict]:
        """Get a single resource by type and ID"""
        return self.resources.get(resource_type, {}).get(resource_id)

    def search_resources(self, resource_type: str, params: dict) -> List[dict]:
        """Search for resources by type and parameters"""
        results = []
        resources = self.resources.get(resource_type, {})

        # Simple search implementation
        for resource_id, resource in resources.items():
            if self._matches_search_params(resource, params):
                results.append(resource)

        return results

    def _matches_search_params(self, resource: dict, params: dict) -> bool:
        """Check if a resource matches search parameters"""
        for param, value in params.items():
            if param == '_id':
                if resource.get('id') != value:
                    return False
            elif param in ['patient', 'subject']:
                subject_ref = resource.get('subject', {}).get('reference', '')
                if not subject_ref.endswith(value) and f"Patient/{value}" != subject_ref:
                    return False
        return True

    def get_patient_everything(self, patient_id: str) -> dict:
        """Get all resources related to a patient"""
        bundle_entries = []

        # Add the patient
        patient = self.get_resource('Patient', patient_id)
        if not patient:
            return None

        bundle_entries.append({
            "fullUrl": f"http://localhost:5000/fhir/Patient/{patient_id}",
            "resource": patient
        })

        # Add all resources that reference this patient
        for resource_type, resources in self.resources.items():
            if resource_type == 'Patient':
                continue

            for resource_id, resource in resources.items():
                # Check if resource references this patient
                if self._resource_references_patient(resource, patient_id):
                    bundle_entries.append({
                        "fullUrl": f"http://localhost:5000/fhir/{resource_type}/{resource_id}",
                        "resource": resource
                    })

        # Create Bundle
        bundle = {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": len(bundle_entries),
            "entry": bundle_entries
        }

        return bundle

    def _resource_references_patient(self, resource: dict, patient_id: str) -> bool:
        """Check if a resource references a specific patient"""
        subject_ref = resource.get('subject', {}).get('reference', '')
        if subject_ref == f"Patient/{patient_id}":
            return True

        # Check patient field
        patient_ref = resource.get('patient', {}).get('reference', '')
        if patient_ref == f"Patient/{patient_id}":
            return True

        return False

    def get_capability_statement(self) -> dict:
        """Generate FHIR Capability Statement"""
        return {
            "resourceType": "CapabilityStatement",
            "status": "active",
            "date": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "kind": "instance",
            "fhirVersion": "4.0.1",
            "format": ["json"],
            "rest": [{
                "mode": "server",
                "resource": [
                    {
                        "type": resource_type,
                        "interaction": [
                            {"code": "read"},
                            {"code": "search-type"}
                        ],
                        "searchParam": [
                            {"name": "_id", "type": "token"},
                            {"name": "patient", "type": "reference"},
                            {"name": "subject", "type": "reference"}
                        ]
                    }
                    for resource_type in self.resources.keys()
                ],
                "operation": [{
                    "name": "everything",
                    "definition": "http://hl7.org/fhir/OperationDefinition/Patient-everything"
                }]
            }]
        }


# Flask application
app = Flask(__name__)
CORS(app)

# Global server instance and config
fhir_server = None
_server_config = {}


def init_server(fhir_data_path: str, notes_data_path: str, mapping_file: str):
    """Initialize the global FHIR server instance"""
    global fhir_server, _server_config

    # Store config in environment variables (survives Flask reload)
    os.environ['MIMIC_FHIR_DATA_PATH'] = fhir_data_path
    os.environ['MIMIC_NOTES_DATA_PATH'] = notes_data_path
    os.environ['MIMIC_MAPPING_FILE'] = mapping_file

    # Store config for lazy initialization
    _server_config = {
        'fhir_data_path': fhir_data_path,
        'notes_data_path': notes_data_path,
        'mapping_file': mapping_file
    }

    # Initialize immediately
    if fhir_server is None:
        fhir_server = MimicFhirServer(**_server_config)


def get_fhir_server():
    """Get or create the FHIR server instance (lazy initialization)"""
    global fhir_server
    if fhir_server is None:
        # Try to initialize from _server_config first
        if _server_config:
            fhir_server = MimicFhirServer(**_server_config)
        # Fallback to environment variables (for Flask reloader child process)
        elif os.environ.get('MIMIC_FHIR_DATA_PATH'):
            fhir_server = MimicFhirServer(
                fhir_data_path=os.environ['MIMIC_FHIR_DATA_PATH'],
                notes_data_path=os.environ['MIMIC_NOTES_DATA_PATH'],
                mapping_file=os.environ.get('MIMIC_MAPPING_FILE', 'fhir_patients_with_notes_mapping.json')
            )
    return fhir_server


@app.route('/fhir/metadata', methods=['GET'])
def capability_statement():
    """Return capability statement"""
    server = get_fhir_server()
    return jsonify(server.get_capability_statement())


@app.route('/fhir/<resource_type>/<resource_id>', methods=['GET'])
def read_resource(resource_type, resource_id):
    """Read a single resource"""
    server = get_fhir_server()
    resource = server.get_resource(resource_type, resource_id)

    if resource:
        return jsonify(resource)
    else:
        return jsonify({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "not-found",
                "diagnostics": f"{resource_type}/{resource_id} not found"
            }]
        }), 404


@app.route('/fhir/<resource_type>', methods=['GET'])
def search_resources(resource_type):
    """Search for resources"""
    server = get_fhir_server()
    logger.info(f"Search request for {resource_type}, server has {len(server.resources.get(resource_type, {}))} resources of this type")
    params = request.args.to_dict()
    resources = server.search_resources(resource_type, params)

    # Create search bundle
    bundle = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": len(resources),
        "entry": [
            {
                "fullUrl": f"http://localhost:5000/fhir/{resource_type}/{r['id']}",
                "resource": r
            }
            for r in resources
        ]
    }

    return jsonify(bundle)


@app.route('/fhir/Patient/<patient_id>/$everything', methods=['GET'])
def patient_everything(patient_id):
    """Get all resources for a patient"""
    server = get_fhir_server()
    bundle = server.get_patient_everything(patient_id)

    if bundle:
        return jsonify(bundle)
    else:
        return jsonify({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "not-found",
                "diagnostics": f"Patient/{patient_id} not found"
            }]
        }), 404


@app.route('/fhir/', methods=['GET'])
@app.route('/fhir', methods=['GET'])
def fhir_root():
    """FHIR server root"""
    return jsonify({
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{
            "fullUrl": "http://localhost:5000/fhir/metadata",
            "resource": {
                "resourceType": "CapabilityStatement",
                "status": "active"
            }
        }]
    })


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='MIMIC-IV FHIR Server')
    parser.add_argument('--fhir-data', required=True, help='Path to FHIR data directory')
    parser.add_argument('--notes-data', required=True, help='Path to notes data directory')
    parser.add_argument('--mapping-file', default='fhir_patients_with_notes_mapping.json',
                        help='Path to patient mapping file')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')

    args = parser.parse_args()

    # Initialize server (will also initialize on reload in debug mode)
    init_server(
        fhir_data_path=args.fhir_data,
        notes_data_path=args.notes_data,
        mapping_file=args.mapping_file
    )

    # Run Flask app
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()

"""
MIMIC-IV FHIR Server Client Example

Demonstrates how to interact with the FHIR server API
"""

import requests
import json
import argparse
from pathlib import Path
from typing import Optional


class FhirClient:
    """Simple FHIR client"""

    def __init__(self, base_url: str = "http://localhost:5000/fhir", mapping_file: str = "fhir_patients_with_notes_mapping.json"):
        self.base_url = base_url.rstrip('/')
        self.mapping_file = mapping_file
        self._subject_to_uuid = None

    def _load_mapping(self):
        """Load subject_id to UUID mapping"""
        if self._subject_to_uuid is None:
            import json
            with open(self.mapping_file, 'r') as f:
                data = json.load(f)
            self._subject_to_uuid = data.get('uuid_to_subject_mapping', {})
            # Invert the mapping
            self._subject_to_uuid = {v: k for k, v in self._subject_to_uuid.items()}
        return self._subject_to_uuid

    def resolve_patient_id(self, patient_id: str) -> str:
        """Convert subject_id to UUID if needed"""
        # If it looks like a UUID (contains hyphens), return as-is
        if '-' in patient_id:
            return patient_id

        # Otherwise, try to convert from subject_id
        mapping = self._load_mapping()
        uuid = mapping.get(patient_id)
        if uuid:
            return uuid

        # If not found in mapping, return as-is (might be a UUID without hyphens or invalid)
        return patient_id

    def get_capability_statement(self) -> dict:
        """Get server capability statement"""
        response = requests.get(f"{self.base_url}/metadata")
        response.raise_for_status()
        return response.json()

    def read_resource(self, resource_type: str, resource_id: str) -> Optional[dict]:
        """Read a single resource"""
        response = requests.get(f"{self.base_url}/{resource_type}/{resource_id}")
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            response.raise_for_status()

    def search_resources(self, resource_type: str, **params) -> dict:
        """Search for resources"""
        response = requests.get(f"{self.base_url}/{resource_type}", params=params)
        response.raise_for_status()
        return response.json()

    def patient_everything(self, patient_id: str) -> dict:
        """Get all resources for a patient (accepts UUID or subject_id)"""
        patient_id = self.resolve_patient_id(patient_id)
        response = requests.get(f"{self.base_url}/Patient/{patient_id}/$everything")
        response.raise_for_status()
        return response.json()

    def save_patient_data(self, patient_id: str, output_dir: str = "mimic-iv-data/patients"):
        """
        Save patient data to files

        Creates two files:
        1. {patient_id}_raw.json - Raw FHIR Bundle from server
        2. {patient_id}_timeline.json - Processed timeline events

        Args:
            patient_id: Patient UUID or subject_id
            output_dir: Directory to save files (default: mimic-iv-data/patients)
        """
        # Resolve patient ID
        original_id = patient_id
        resolved_id = self.resolve_patient_id(patient_id)

        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Get raw bundle from server
        print(f"Fetching data for patient {original_id}...")
        if resolved_id != original_id:
            print(f"  Resolved to UUID: {resolved_id}")

        bundle = self.patient_everything(patient_id)

        # Save raw bundle
        raw_file = output_path / f"{resolved_id}_raw.json"
        with open(raw_file, 'w') as f:
            json.dump(bundle, f, indent=2)
        print(f"  Saved raw bundle to: {raw_file}")

        # Process timeline
        timeline_data = self._process_timeline(bundle, resolved_id)

        # Save timeline
        timeline_file = output_path / f"{resolved_id}_timeline.json"
        with open(timeline_file, 'w') as f:
            json.dump(timeline_data, f, indent=2)
        print(f"  Saved timeline to: {timeline_file}")

        return {
            'patient_id': resolved_id,
            'original_id': original_id,
            'raw_file': str(raw_file),
            'timeline_file': str(timeline_file),
            'total_resources': bundle.get('total', 0),
            'timeline_summary': timeline_data.get('summary', {})
        }

    def _process_timeline(self, bundle: dict, patient_id: str) -> dict:
        """Process FHIR bundle into timeline events organized by encounter"""
        # First pass: collect all encounters and create a map
        encounters = {}
        encounter_order = []

        for entry in bundle.get('entry', []):
            resource = entry.get('resource', {})
            if resource.get('resourceType') == 'Encounter':
                encounter_id = resource.get('id')
                period = resource.get('period', {})
                encounters[encounter_id] = {
                    'id': encounter_id,
                    'start': period.get('start'),
                    'end': period.get('end'),
                    'class': resource.get('class', {}).get('display', 'Unknown'),
                    'type': resource.get('type', [{}])[0].get('coding', [{}])[0].get('display', ''),
                    'events': []
                }
                encounter_order.append(encounter_id)

        # Sort encounters by start date
        encounter_order.sort(key=lambda eid: encounters[eid]['start'] or '')

        # Second pass: assign events to encounters
        non_encounter_events = []
        patient_demographics = None

        for entry in bundle.get('entry', []):
            resource = entry.get('resource', {})
            resource_type = resource.get('resourceType')

            # Extract encounter reference if present
            encounter_ref = None
            if 'encounter' in resource:
                ref = resource['encounter'].get('reference', '')
                encounter_ref = ref.split('/')[-1] if '/' in ref else ref
            elif 'context' in resource:
                context = resource['context']
                # Check for context.encounter (list format)
                if 'encounter' in context:
                    encounters_list = context['encounter']
                    if encounters_list:
                        ref = encounters_list[0].get('reference', '')
                        encounter_ref = ref.split('/')[-1] if '/' in ref else ref
                # Check for context.reference (direct format - used by MedicationDispense, etc.)
                elif 'reference' in context:
                    ref = context.get('reference', '')
                    if 'Encounter/' in ref:
                        encounter_ref = ref.split('/')[-1] if '/' in ref else ref

            # Build event object
            event = self._create_event_from_resource(resource)

            if event:
                # Add encounter_referenced field
                event['encounter_referenced'] = encounter_ref if encounter_ref else 'none'

                # Patient demographics are special
                if resource_type == 'Patient':
                    patient_demographics = event
                # Assign to encounter if we have a reference
                elif encounter_ref and encounter_ref in encounters:
                    encounters[encounter_ref]['events'].append(event)
                # Otherwise add to non-encounter events for now
                else:
                    non_encounter_events.append(event)

        # Third pass: assign non-encounter events to encounters by time window
        remaining_non_encounter_events = []
        for event in non_encounter_events:
            event_date = event.get('date')
            assigned = False

            if event_date:
                # Try to find an encounter that contains this event's timestamp
                for encounter_id, encounter in encounters.items():
                    start = encounter['start']
                    end = encounter['end']

                    # Check if event falls within encounter time window
                    if start and end and start <= event_date <= end:
                        encounter['events'].append(event)
                        assigned = True
                        break
                    # If no end time, check if event is after start
                    elif start and not end and event_date >= start:
                        encounter['events'].append(event)
                        assigned = True
                        break

            # If not assigned to any encounter, keep in non-encounter list
            if not assigned:
                remaining_non_encounter_events.append(event)

        non_encounter_events = remaining_non_encounter_events

        # Sort events within each encounter by date and split into dated/undated
        for encounter in encounters.values():
            # Sort all events first
            encounter['events'].sort(key=lambda x: (x.get('date') is None, x.get('date', '')))

            # Split into dated and undated lists
            dated_events = [e for e in encounter['events'] if not e.get('undated')]
            undated_events = [e for e in encounter['events'] if e.get('undated')]

            encounter['dated_events'] = dated_events
            encounter['undated_events'] = undated_events

        # Sort and split non-encounter events by dated/undated
        non_encounter_events.sort(key=lambda x: (x.get('date') is None, x.get('date', '')))
        dated_non_encounter = [e for e in non_encounter_events if not e.get('undated')]
        undated_non_encounter = [e for e in non_encounter_events if e.get('undated')]

        # Build the structured timeline
        timeline = {
            'patient_id': patient_id,
            'demographics': patient_demographics,
            'encounters': []
        }

        # Add encounters in chronological order with separate event lists
        for encounter_id in encounter_order:
            enc = encounters[encounter_id]
            timeline['encounters'].append({
                'encounter_id': enc['id'],
                'start': enc['start'],
                'end': enc['end'],
                'class': enc['class'],
                'type': enc['type'],
                'dated_event_count': len(enc['dated_events']),
                'undated_event_count': len(enc['undated_events']),
                'events': enc['dated_events'],
                'undated_events': enc['undated_events']
            })

        # Add non-encounter events (split into dated/undated)
        timeline['non_encounter_events'] = dated_non_encounter
        timeline['non_encounter_undated_events'] = undated_non_encounter

        # Summary statistics
        total_dated = sum(len(e['dated_events']) for e in encounters.values()) + len(dated_non_encounter)
        total_undated = sum(len(e['undated_events']) for e in encounters.values()) + len(undated_non_encounter)
        total_events = total_dated + total_undated
        if patient_demographics:
            total_events += 1

        timeline['summary'] = {
            'total_encounters': len(timeline['encounters']),
            'total_events': total_events,
            'total_dated_events': total_dated,
            'total_undated_events': total_undated,
            'events_with_encounter': sum(len(e['dated_events']) + len(e['undated_events']) for e in encounters.values()),
            'events_without_encounter': len(dated_non_encounter) + len(undated_non_encounter)
        }

        return timeline

    def _extract_timestamp_from_resource(self, resource: dict) -> str:
        """
        Extract timestamp from FHIR resource using standard field names.
        Returns the first timestamp found, or None if no timestamp exists.
        Priority order follows FHIR R4 conventions for clinical relevance.
        """
        # Priority list of timestamp fields to check
        timestamp_fields = [
            # Most common clinical timing fields
            'effectiveDateTime', 'effectiveInstant',
            'performedDateTime',
            'authoredOn', 'issued', 'date',
            'recordedDate', 'recorded',
            'onsetDateTime', 'abatementDateTime',
            'occurrenceDateTime',
            'dateAsserted', 'assertedDate',
            'whenHandedOver', 'whenPrepared',
            'created', 'birthDate', 'deceasedDateTime',
            # Period fields - extract start time
            ('effectivePeriod', 'start'),
            ('performedPeriod', 'start'),
            ('period', 'start'),
            ('onsetPeriod', 'start'),
            ('abatementPeriod', 'start'),
            ('occurrencePeriod', 'start'),
            # Nested collection/specimen fields
            ('collection', 'collectedDateTime'),
            ('collection', 'collectedPeriod', 'start'),
        ]

        for field in timestamp_fields:
            if isinstance(field, tuple):
                # Nested field (e.g., period.start or collection.collectedDateTime)
                if len(field) == 2:
                    parent_field, child_field = field
                    if parent_field in resource and isinstance(resource[parent_field], dict):
                        timestamp = resource[parent_field].get(child_field)
                        if timestamp:
                            return timestamp
                elif len(field) == 3:
                    # Double nested (e.g., collection.collectedPeriod.start)
                    parent_field, middle_field, child_field = field
                    if parent_field in resource and isinstance(resource[parent_field], dict):
                        middle = resource[parent_field].get(middle_field)
                        if middle and isinstance(middle, dict):
                            timestamp = middle.get(child_field)
                            if timestamp:
                                return timestamp
            else:
                # Direct field
                timestamp = resource.get(field)
                if timestamp:
                    return timestamp

        return None

    def _extract_code_info(self, resource: dict) -> dict:
        """Extract code/coding information from FHIR resource for display"""
        code_info = {}

        # Try to find code in common locations
        if 'code' in resource:
            code_obj = resource['code']
            if isinstance(code_obj, dict) and 'coding' in code_obj:
                coding = code_obj['coding'][0] if code_obj['coding'] else {}
                code_info['code_display'] = coding.get('display')
                code_info['code'] = coding.get('code')
                code_info['code_system'] = coding.get('system')
            elif isinstance(code_obj, dict) and 'text' in code_obj:
                code_info['code_display'] = code_obj['text']

        # For resources with 'type' instead of 'code'
        if not code_info and 'type' in resource:
            type_obj = resource['type']
            if isinstance(type_obj, list) and type_obj:
                type_obj = type_obj[0]
            if isinstance(type_obj, dict) and 'coding' in type_obj:
                coding = type_obj['coding'][0] if type_obj['coding'] else {}
                code_info['code_display'] = coding.get('display')
                code_info['code'] = coding.get('code')
                code_info['code_system'] = coding.get('system')

        return code_info

    def _create_event_from_resource(self, resource: dict) -> dict:
        """Create an event object from a FHIR resource using actual FHIR fields (no synthetic concatenation)"""
        resource_type = resource.get('resourceType')
        date = None
        category = None
        details = {}

        if resource_type == 'Patient':
            # Extract comprehensive demographics
            date = resource.get('birthDate')
            category = 'demographics'

            # Basic demographics
            details['birth_date'] = resource.get('birthDate')
            details['deceased_date'] = resource.get('deceasedDateTime')
            details['gender'] = resource.get('gender')

            # Subject ID from identifier
            for identifier in resource.get('identifier', []):
                if identifier.get('system') == 'http://mimic.mit.edu/fhir/mimic/identifier/patient':
                    details['subject_id'] = identifier.get('value')

            # Name
            names = resource.get('name', [])
            if names:
                name_obj = names[0]
                details['family_name'] = name_obj.get('family')
                details['given_names'] = name_obj.get('given', [])
                details['name_use'] = name_obj.get('use')

            # Race (from US Core extension)
            for ext in resource.get('extension', []):
                if 'us-core-race' in ext.get('url', ''):
                    for sub_ext in ext.get('extension', []):
                        if sub_ext.get('url') == 'text':
                            details['race'] = sub_ext.get('valueString')
                        elif sub_ext.get('url') == 'ombCategory':
                            details['race_code'] = sub_ext.get('valueCoding', {}).get('code')

            # Ethnicity (from US Core extension)
            for ext in resource.get('extension', []):
                if 'us-core-ethnicity' in ext.get('url', ''):
                    for sub_ext in ext.get('extension', []):
                        if sub_ext.get('url') == 'text':
                            details['ethnicity'] = sub_ext.get('valueString')
                        elif sub_ext.get('url') == 'ombCategory':
                            details['ethnicity_code'] = sub_ext.get('valueCoding', {}).get('code')

            # Birth sex (from US Core extension)
            for ext in resource.get('extension', []):
                if 'us-core-birthsex' in ext.get('url', ''):
                    details['birth_sex'] = ext.get('valueCode')

            # Marital status
            marital_status = resource.get('maritalStatus', {}).get('coding', [])
            if marital_status:
                marital_code = marital_status[0].get('code')
                # Map common codes to readable names
                marital_map = {
                    'S': 'Single',
                    'M': 'Married',
                    'D': 'Divorced',
                    'W': 'Widowed',
                    'L': 'Legally Separated',
                    'A': 'Annulled',
                    'P': 'Polygamous',
                    'T': 'Domestic Partner',
                    'U': 'Unmarried',
                    'UNK': 'Unknown'
                }
                details['marital_status'] = marital_map.get(marital_code, marital_code)
                details['marital_status_code'] = marital_code

            # Communication/Language
            communications = resource.get('communication', [])
            if communications:
                languages = []
                for comm in communications:
                    lang_coding = comm.get('language', {}).get('coding', [])
                    if lang_coding:
                        languages.append(lang_coding[0].get('code'))
                if languages:
                    details['languages'] = languages

        elif resource_type == 'Encounter':
            # Encounters are handled separately
            return None

        elif resource_type == 'Condition':
            date = resource.get('onsetDateTime') or resource.get('recordedDate')
            category = 'condition'

            # Extract code display from FHIR coding
            coding = resource.get('code', {}).get('coding', [{}])[0]
            details['code_display'] = coding.get('display')
            details['code'] = coding.get('code')
            details['code_system'] = coding.get('system')

        elif resource_type == 'Procedure':
            date = resource.get('performedDateTime')
            category = 'procedure'

            # Extract code display from FHIR coding
            coding = resource.get('code', {}).get('coding', [{}])[0]
            details['code_display'] = coding.get('display')
            details['code'] = coding.get('code')
            details['code_system'] = coding.get('system')

        elif resource_type == 'DocumentReference':
            date = resource.get('date')
            category = 'document'

            # Extract document type from FHIR coding
            type_coding = resource.get('type', {}).get('coding', [{}])[0]
            details['document_type'] = type_coding.get('display')
            details['document_type_code'] = type_coding.get('code')
            details['document_type_system'] = type_coding.get('system')

            # Extract clinical note text
            content = resource.get('content', [])
            if content and len(content) > 0:
                attachment = content[0].get('attachment', {})
                note_text = attachment.get('data', '')
                if note_text:
                    details['note_text'] = note_text
                    # Add preview for quick scanning
                    preview = note_text[:100].replace('\n', ' ').strip()
                    if len(note_text) > 100:
                        preview += '...'
                    details['note_preview'] = preview

        elif resource_type == 'Observation':
            date = resource.get('effectiveDateTime')
            category = 'observation'

            # Extract code display from FHIR coding
            coding = resource.get('code', {}).get('coding', [{}])[0]
            details['code_display'] = coding.get('display')
            details['code'] = coding.get('code')
            details['code_system'] = coding.get('system')

            # Extract value (either quantity or string)
            value_quantity = resource.get('valueQuantity', {})
            if value_quantity:
                details['value'] = value_quantity.get('value')
                details['unit'] = value_quantity.get('unit')

            value_string = resource.get('valueString')
            if value_string:
                details['value_string'] = value_string

            # Extract note if present
            notes = resource.get('note', [])
            if notes and len(notes) > 0:
                details['note'] = notes[0].get('text')

        elif resource_type == 'MedicationRequest':
            date = resource.get('authoredOn')
            category = 'medication'

            # Extract medication reference or codeable concept
            med_ref = resource.get('medicationReference')
            if med_ref:
                details['medication_reference'] = med_ref.get('reference')

            med_codeable = resource.get('medicationCodeableConcept', {})
            if med_codeable:
                coding = med_codeable.get('coding', [{}])[0]
                details['medication_display'] = coding.get('display')
                details['medication_code'] = coding.get('code')
                details['medication_system'] = coding.get('system')

            # Extract dosage instruction if present
            dosage_instructions = resource.get('dosageInstruction', [])
            if dosage_instructions and len(dosage_instructions) > 0:
                dosage = dosage_instructions[0]
                details['dosage_text'] = dosage.get('text')

                dose_and_rate = dosage.get('doseAndRate', [])
                if dose_and_rate and len(dose_and_rate) > 0:
                    dose_quantity = dose_and_rate[0].get('doseQuantity', {})
                    details['dose_value'] = dose_quantity.get('value')
                    details['dose_unit'] = dose_quantity.get('unit')

                route = dosage.get('route', {}).get('coding', [{}])[0]
                if route:
                    details['route_code'] = route.get('code')

                timing_code = dosage.get('timing', {}).get('code', {}).get('coding', [{}])[0]
                if timing_code:
                    details['timing'] = timing_code.get('code')

        else:
            # Generic fallback handler for all other FHIR resource types
            # This allows any resource with a timestamp to be included
            date = self._extract_timestamp_from_resource(resource)
            category = resource_type.lower() if resource_type else 'unknown'

            # Extract code information if available
            code_info = self._extract_code_info(resource)
            if code_info:
                details.update(code_info)

            # Extract common fields that might be useful
            if 'status' in resource:
                details['status'] = resource['status']

            # For resources with a 'text' narrative
            if 'text' in resource and isinstance(resource['text'], dict):
                if 'div' in resource['text']:
                    # HTML narrative - extract text preview
                    div_text = resource['text']['div']
                    # Simple HTML tag removal for preview
                    import re
                    text_preview = re.sub(r'<[^>]+>', '', div_text)
                    text_preview = text_preview.strip()[:200]
                    if text_preview:
                        details['text_preview'] = text_preview

        # Create event if we have a date OR if there's an encounter/context reference
        # Resources without dates but with encounter refs will be marked as undated
        has_encounter_ref = 'encounter' in resource or 'context' in resource

        if date or (has_encounter_ref and category):
            event = {
                'date': date,
                'undated': not bool(date),
                'type': resource_type,
                'category': category,
                'resource_id': resource.get('id')
            }
            if details:
                event['details'] = details
            return event

        return None


def print_json(data: dict, indent: int = 2):
    """Pretty print JSON"""
    print(json.dumps(data, indent=indent))


def demo_basic_operations(client: FhirClient):
    """Demonstrate basic FHIR operations"""
    print("=" * 80)
    print("MIMIC-IV FHIR Server Client Demo")
    print("=" * 80)
    print()

    # 1. Get capability statement
    print("1. Getting Capability Statement...")
    print("-" * 80)
    capability = client.get_capability_statement()
    print(f"Server FHIR Version: {capability.get('fhirVersion')}")
    print(f"Supported Resource Types: {len(capability['rest'][0]['resource'])}")
    print()

    # 2. Read a specific patient
    print("2. Reading a Patient resource...")
    print("-" * 80)
    patient_id = "0a8eebfd-a352-522e-89f0-1d4a13abdebc"  # Patient 10000032
    patient = client.read_resource('Patient', patient_id)

    if patient:
        print(f"Patient ID: {patient['id']}")
        print(f"Gender: {patient.get('gender')}")
        print(f"Birth Date: {patient.get('birthDate')}")

        # Get subject_id from identifier
        for identifier in patient.get('identifier', []):
            if identifier.get('system') == 'http://mimic.mit.edu/fhir/mimic/identifier/patient':
                print(f"Subject ID: {identifier.get('value')}")
    else:
        print(f"Patient {patient_id} not found")
    print()

    # 3. Search for patients
    print("3. Searching for Patient resources...")
    print("-" * 80)
    bundle = client.search_resources('Patient')
    print(f"Total patients: {bundle.get('total')}")
    print()

    # 4. Get patient $everything
    print("4. Getting Patient $everything...")
    print("-" * 80)
    everything_bundle = client.patient_everything(patient_id)
    print(f"Total resources for patient: {everything_bundle.get('total')}")

    # Count by resource type
    resource_counts = {}
    for entry in everything_bundle.get('entry', []):
        resource_type = entry['resource'].get('resourceType')
        resource_counts[resource_type] = resource_counts.get(resource_type, 0) + 1

    print("\nResources by type:")
    for resource_type, count in sorted(resource_counts.items()):
        print(f"  {resource_type}: {count}")
    print()

    # 5. Get DocumentReferences
    print("5. Searching for DocumentReferences for this patient...")
    print("-" * 80)
    doc_bundle = client.search_resources('DocumentReference', patient=patient_id)
    doc_refs = doc_bundle.get('entry', [])
    print(f"Total DocumentReferences: {len(doc_refs)}")

    if doc_refs:
        print("\nFirst DocumentReference:")
        first_doc = doc_refs[0]['resource']
        print(f"  ID: {first_doc['id']}")
        print(f"  Type: {first_doc['type']['coding'][0]['display']}")
        print(f"  Date: {first_doc.get('date')}")
        print(f"  Has content: {len(first_doc.get('content', []))} attachment(s)")
    print()

    # 6. Get observations
    print("6. Searching for Observations for this patient...")
    print("-" * 80)
    obs_bundle = client.search_resources('Observation', patient=patient_id)
    observations = obs_bundle.get('entry', [])
    print(f"Total Observations: {len(observations)}")

    if observations:
        print("\nFirst 3 Observations:")
        for i, entry in enumerate(observations[:3]):
            obs = entry['resource']
            code_display = obs.get('code', {}).get('coding', [{}])[0].get('display', 'Unknown')
            value = obs.get('valueQuantity', {}).get('value', 'N/A')
            unit = obs.get('valueQuantity', {}).get('unit', '')
            print(f"  {i+1}. {code_display}: {value} {unit}")
    print()

    print("=" * 80)
    print("Demo Complete")
    print("=" * 80)


def generate_patient_timeline(client: FhirClient, patient_id: str):
    """Generate a timeline of events for a patient"""
    print("=" * 80)
    print(f"Patient Timeline for {patient_id}")
    print("=" * 80)
    print()

    # Resolve patient ID (convert subject_id to UUID if needed)
    resolved_id = client.resolve_patient_id(patient_id)
    if resolved_id != patient_id:
        print(f"Resolved subject_id {patient_id} to UUID {resolved_id}")
        print()

    # Get all patient data
    bundle = client.patient_everything(patient_id)

    # Extract and sort events by date
    events = []

    for entry in bundle.get('entry', []):
        resource = entry['resource']
        resource_type = resource.get('resourceType')

        # Extract date based on resource type
        date = None
        description = None

        if resource_type == 'Patient':
            date = resource.get('birthDate')
            description = f"Patient Birth"

        elif resource_type == 'Encounter':
            period = resource.get('period', {})
            date = period.get('start')
            encounter_class = resource.get('class', {}).get('display', 'Unknown')
            description = f"Encounter: {encounter_class}"

        elif resource_type == 'Condition':
            code = resource.get('code', {}).get('coding', [{}])[0].get('display', 'Unknown condition')
            description = f"Condition: {code}"

        elif resource_type == 'Procedure':
            date = resource.get('performedDateTime')
            code = resource.get('code', {}).get('coding', [{}])[0].get('display', 'Unknown procedure')
            description = f"Procedure: {code}"

        elif resource_type == 'DocumentReference':
            date = resource.get('date')
            doc_type = resource.get('type', {}).get('coding', [{}])[0].get('display', 'Document')
            description = f"Document: {doc_type}"

        elif resource_type == 'Observation':
            date = resource.get('effectiveDateTime')
            code = resource.get('code', {}).get('coding', [{}])[0].get('display', 'Observation')
            value = resource.get('valueQuantity', {}).get('value', '')
            unit = resource.get('valueQuantity', {}).get('unit', '')
            description = f"Lab: {code} = {value} {unit}"

        if date and description:
            events.append({
                'date': date,
                'type': resource_type,
                'description': description
            })

    # Sort by date
    events.sort(key=lambda x: x['date'])

    # Print timeline (limit to first 20 events)
    print(f"Showing first 20 of {len(events)} events:\n")
    for event in events[:20]:
        print(f"{event['date']}: {event['description']}")

    print()
    print("=" * 80)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='MIMIC-IV FHIR Client Example')
    parser.add_argument('--base-url', default='http://localhost:5000/fhir',
                        help='FHIR server base URL')
    parser.add_argument('--patient-id', help='Patient ID (UUID or subject_id) for operations')
    parser.add_argument('--generate-timelines', action='store_true',
                        help='Generate timelines for all patients')
    parser.add_argument('--save', action='store_true',
                        help='Save patient data to files (use with --patient-id)')
    parser.add_argument('--output-dir', default='mimic-iv-data/patients',
                        help='Output directory for saved patient data (default: mimic-iv-data/patients)')

    args = parser.parse_args()

    client = FhirClient(args.base_url)

    try:
        if args.save and args.patient_id:
            # Save patient data to files
            result = client.save_patient_data(args.patient_id, args.output_dir)
            summary = result['timeline_summary']
            print(f"\nSummary:")
            print(f"  Patient ID: {result['patient_id']}")
            if result['original_id'] != result['patient_id']:
                print(f"  Original ID: {result['original_id']}")
            print(f"  Total Resources: {result['total_resources']}")
            print(f"  Total Encounters: {summary.get('total_encounters', 0)}")
            print(f"  Total Events: {summary.get('total_events', 0)}")
            print(f"    - With Encounter: {summary.get('events_with_encounter', 0)}")
            print(f"    - Without Encounter: {summary.get('events_without_encounter', 0)}")
            print(f"  Raw Bundle: {result['raw_file']}")
            print(f"  Timeline: {result['timeline_file']}")

        elif args.generate_timelines:
            # Get all patients and generate timeline for each
            bundle = client.search_resources('Patient')
            patients = [entry['resource'] for entry in bundle.get('entry', [])]

            for i, patient in enumerate(patients[:5]):  # Limit to first 5
                print(f"\nGenerating timeline {i+1}/{min(5, len(patients))}...")
                generate_patient_timeline(client, patient['id'])
                print()

        elif args.patient_id:
            generate_patient_timeline(client, args.patient_id)

        else:
            demo_basic_operations(client)

    except requests.exceptions.ConnectionError:
        print("\nERROR: Could not connect to FHIR server")
        print(f"Make sure the server is running at {args.base_url}")
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()

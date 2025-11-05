## Setup Instructions
0. Clone the repository
0. Install mimic IV deidentified clincal notes and FHIR mapping to the mimic-iv-data/
    - Clinic notes: https://physionet.org/content/mimic-iv-note/2.2/#files-panel 
    - FHIR mapping: https://physionet.org/content/mimic-iv-fhir-demo/2.1.0/#files-panel
1. Create a new virtual environment: python -m venv fhir-server-venv and activate the venv
2. With venv active: pip install -r requirements.txt
3. run python start_server.py (note the file paths within this file and adjust if needed)
4. In a seperate terminal, run client_example.py --patient-id [patient-id from fhir_patients_with_notes_mapping.json] --generate-timelines --save

## Output structure
For each client run, there should be 2 files generated:
- *_raw.json: This is the raw response from the FHIR server to the querrying client.
- *_timeline.json: This is the output of the example client after it processes the raw response. It organizes the response into dated encounters. Within each encounter is a series of events(dated) that are associated with that encounter or took place during the encounter period and a series of undated_events that do not have a date but are referencing the encounter.

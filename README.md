0. Clone the repository
0. Install mimic IV deidentified clincal notes and FHIR mapping to the mimic-iv-data/
    - Clinic notes: https://physionet.org/content/mimic-iv-note/2.2/#files-panel 
    - FHIR mapping: https://physionet.org/content/mimic-iv-fhir-demo/2.1.0/#files-panel
1. Create a new virtual environment: python -m venv fhir-server-venv and activate the venv
2. With venv active: pip install -r requirements.txt
3. run python start_server.py (note the file paths within this file and adjust if needed)
4. In a seperate terminal, run client_example.py --patient-id [patient-id from fhir_patients_with_notes_mapping.json] --generate-timelines --save
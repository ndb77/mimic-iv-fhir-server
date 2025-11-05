"""
Simple launcher script for MIMIC-IV FHIR Server
"""

import subprocess
import sys

def main():
    """Start the FHIR server with default configuration"""
    cmd = [
        sys.executable,
        "mimic_fhir_server.py",
        "--fhir-data", "mimic-iv-data/mimic-iv-clinical-database-demo-on-fhir-2.1.0/mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir",
        "--notes-data", "mimic-iv-data"
    ]

    print("Starting MIMIC-IV FHIR Server...")
    print(f"Command: {' '.join(cmd)}")
    print()

    subprocess.run(cmd)


if __name__ == '__main__':
    main()

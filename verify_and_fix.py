import json
import subprocess
import sys

# Load audit file
with open('data/verified/audit_exam2_q16-30.json', 'r') as f:
    audit = json.load(f)

# Questions to fix (based on earlier verify output)
failing_ids = [16, 19, 20, 21, 22, 24, 25, 26, 27, 28, 29, 30]

for entry in audit['audit']:
    qid = entry['id']
    if qid not in failing_ids:
        continue
    
    url = entry['evidence_url']
    print(f"\n=== Q{qid} ===")
    print(f"URL: {url}")
    
    # Fetch the page to find evidence
    result = subprocess.run(['python3', 'scripts/doc_text.py', url], 
                          capture_output=True, text=True, timeout=10)
    if result.returncode != 0:
        print("ERROR: Could not fetch")
        continue
    
    page_text = result.stdout
    print(f"Page fetched ({len(page_text)} chars)")
    
    # Show first 300 chars for context
    print("First excerpt:")
    print(page_text[:500])


import json
import os

os.chdir(os.path.dirname(__file__))
with open('data/merged_final.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

scheduled = [r for r in data if (r.get('status') or '').strip().lower() == 'scheduled']
print('scheduled_count=', len(scheduled))
if scheduled:
    os.makedirs('data', exist_ok=True)
    with open('data/scheduled_records.json', 'w', encoding='utf-8') as f:
        json.dump(scheduled, f, indent=2)
    print('wrote data/scheduled_records.json')
else:
    print('no scheduled records found')

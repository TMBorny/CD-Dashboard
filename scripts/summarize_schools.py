from pathlib import Path
import json

schools_path = Path(__file__).resolve().with_name("schools.json")
with schools_path.open() as f:
    schools = json.load(f)

for slug, name in sorted(schools, key=lambda x: x[0]):
    print(f"{slug} : {name}")

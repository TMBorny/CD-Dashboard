import json
with open("schools.json") as f:
    schools = json.load(f)

for slug, name in sorted(schools, key=lambda x: x[0]):
    print(f"{slug} : {name}")

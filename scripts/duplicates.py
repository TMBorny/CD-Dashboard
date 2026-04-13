from pathlib import Path
import re

school_names_path = Path(__file__).resolve().parents[1] / "src" / "utils" / "schoolNames.ts"
text = school_names_path.read_text()

match = re.search(r"const SCHOOL_NAME_ALIASES: Record<string, string> = \{([^}]+)\}", text)
if match:
    keys = set()
    for i, line in enumerate(match.group(1).split("\n"), start=1):
        key_match = re.search(r"^\s*([A-Za-z0-9_]+):", line)
        if key_match:
            key = key_match.group(1)
            if key in keys:
                print(f"Duplicate: {key} (in dict line {i})")
            keys.add(key)
else:
    print(f"Could not find SCHOOL_NAME_ALIASES in {school_names_path}")

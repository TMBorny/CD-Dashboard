import re
with open('src/utils/schoolNames.ts') as f:
    text = f.read()

match = re.search(r'const FULL_SLUG_ALIASES: Record<string, string> = \{([^}]+)\}', text)
if match:
    keys = set()
    for i, line in enumerate(match.group(1).split('\n')):
        key_match = re.search(r"^\s*'([^']+)'", line)
        if key_match:
            key = key_match.group(1)
            if key in keys:
                print(f"Duplicate: {key} (in dict line {i})")
            keys.add(key)

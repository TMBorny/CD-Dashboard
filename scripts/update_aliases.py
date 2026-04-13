from pathlib import Path
import re

aliases = {
    "aamu": "Alabama A&M University",
    "acu": "Abilene Christian University",
    "alcorn": "Alcorn State University",
    "asub": "Arkansas State University-Beebe",
    "bhcc": "Bunker Hill Community College",
    "ccsj": "Calumet College of St. Joseph",
    "csustan": "CSU Stanislaus",
    "csusm": "CSU San Marcos",
    "cvcc": "Central Virginia Community College",
    "dsu": "Delaware State University",
    "ecok": "East Central University",
    "famu": "Florida A&M University",
    "fpu": "Fresno Pacific University",
    "fscj": "Florida State College Jacksonville",
    "gu": "Gonzaga University",
    "hsus": "Hardin-Simmons University",
    "jsu": "Jacksonville State University",
    "kwu": "Kansas Wesleyan University",
    "mhu": "Mars Hill University",
    "mga": "Middle Georgia State University",
    "ndsu": "North Dakota State University",
    "njcu": "New Jersey City University",
    "nmt": "New Mexico Tech",
    "nscc": "North Shore Community College",
    "oxy": "Occidental College",
    "pfw": "Purdue Fort Wayne",
    "ptc": "Piedmont Technical College",
    "rcbc": "Rowan College at Burlington County",
    "rcsj": "Rowan College of South Jersey",
    "rdpolytech": "Red Deer Polytechnic",
    "rsu": "Rogers State University",
    "saic": "School of the Art Institute of Chicago",
    "sbts": "Southern Baptist Theological Seminary",
    "scsc": "Southern Connecticut State University",
    "scuhs": "Southern California University of Health Sciences",
    "sebts": "Southeastern Baptist Theological Seminary",
    "senmc": "Southeast New Mexico College",
    "seu": "Southeastern University",
    "siu": "Southern Illinois University",
    "siue": "Southern Illinois University Edwardsville",
    "sju": "Saint Joseph's University",
    "smcvt": "Saint Michael's College",
    "svu": "Southern Virginia University",
    "swlaw": "Southwestern Law School",
    "tccd": "Tarrant County College District",
    "trentu": "Trent University",
    "tru": "Thompson Rivers University",
    "ttu": "Texas Tech University",
    "tvcc": "Trinity Valley Community College",
    "uah": "University of Alabama in Huntsville",
    "uclawsf": "UC Law SF",
    "ucr": "UC Riverside",
    "ufv": "University of the Fraser Valley",
    "unf": "University of North Florida",
    "upike": "University of Pikeville",
    "uscga": "US Coast Guard Academy",
    "utulsa": "University of Tulsa",
    "vuu": "Virginia Union University",
    "waynecc": "Wayne Community College",
    "wctc": "Waukesha County Technical College",
    "wcu": "West Coast University",
    "wnc": "Western Nevada College",
    "wne": "Western New England University",
    "wsc": "Wayne State College",
    "wsutech": "WSU Tech",
}

school_names_path = Path(__file__).resolve().parents[1] / "src" / "utils" / "schoolNames.ts"
content = school_names_path.read_text()

# Find the TOKEN_ALIASES object
match = re.search(r"const TOKEN_ALIASES: Record<string, string> = {([\s\S]*?)};", content)
if match:
    existing_content = match.group(1)
    # Add our new aliases if they don't exist
    new_lines = []
    for k, v in aliases.items():
        if f"'{k}':" not in existing_content and f"\"{k}\":" not in existing_content and f"{k}:" not in existing_content:
            new_lines.append(f"  '{k}': '{v}',")
    
    if new_lines:
        updated_block = existing_content.rstrip()
        if not updated_block.endswith(","):
            if updated_block:
                updated_block += ","
        updated_block += "\n" + "\n".join(new_lines) + "\n"
        
        content = content.replace(existing_content, updated_block)
        school_names_path.write_text(content)
        print(f"Added {len(new_lines)} aliases")
    else:
        print("No new aliases to add")
else:
    print("Could not find TOKEN_ALIASES map")

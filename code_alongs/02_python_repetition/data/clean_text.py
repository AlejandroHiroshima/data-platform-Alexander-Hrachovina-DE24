import re
from pathlib import Path

print("Yhis is path of script")
print(Path(__file__).parent / "data" )
with open("data/ml_text_raw.txt", 'r') as file:
    raw_text = file.read()
    
text_fixed_spacing = re.sub(r"\s+", " ",raw_text)

# similar code as in jupyter notevook for cleaning the rest of the text
print(text_fixed_spacing)
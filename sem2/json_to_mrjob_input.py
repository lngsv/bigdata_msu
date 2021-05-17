import json
import sys

filename = sys.argv[1]
jsonstr = ''
with open(filename) as f:
    jsonstr = f.read()

info = json.loads(jsonstr)

for item in info:
    print(json.dumps(item, ensure_ascii=False))

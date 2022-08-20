import json

with open('tmpfile', 'w') as f:
    json.dump({"hello-world": "hello!"}, f)
    f.close()
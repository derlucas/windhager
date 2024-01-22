import json
import re


with open("de-parameters.json") as f:
    pattern = re.compile("^EmStrId_FE([0-9]+)+_TEXT")
    data = json.load(f).get('emStrIds')

    errors = [x for x in data if pattern.match(x)]

    output = []

    for x in errors:
        number = pattern.search(x).group(1)

        obj = {
                "options": {
                    number: {
                        "color": "yellow",
                        "index": 0,
                        "text": data.get(x)
                    }
                },
                "type": "value"
              }

        output.append(obj)

    print(json.dumps(output))


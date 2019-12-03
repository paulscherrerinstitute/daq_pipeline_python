wimport json
from copy import deepcopy

with open("pod_template.json") as input_file:
    template = json.load(input_file)

with open("../docker/sources.json") as input_file:
    data = json.load(input_file)

for device in data.keys():
    device_file = deepcopy(template)
    device_file["metadata"]["name"] = device.lower()

    device_file["spec"]["containers"][0]["env"].append(
        {
            "name": "DEVICE_NAME",
            "value": device
        })

    with open("sources/%s.json" % device, 'w') as output_file:
        json.dump(device_file, output_file, indent=4)

import argparse
import json
from copy import deepcopy

parser = argparse.ArgumentParser()

parser.add_argument('--source_file', type=str, default="sources_all.json",
                    help='Simulated device name')

args = parser.parse_args()

with open("pod_template.json") as input_file:
    template = json.load(input_file)

with open(args.source_file) as input_file:
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

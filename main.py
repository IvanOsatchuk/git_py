import json
import os

import requests
import yaml

def teste():
    print('hello')

def git_call(msg, branch):
    os.system(f"git pull")
    os.system(f"git checkout -b {branch}")
    os.system(f"git commit -m '{msg}' -a")
    os.system(f"git push --set-upstream origin {branch}")
    
    

# Write the pipelline config out to a file
def exportPipeline(ns, id, data):
    fileName = id + ".json"
    directory = "datafusion/" + ns
    path = directory + "/" + fileName

    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(path, "w") as f:
        f.write(data)


def write_config_file(data, namespace, pipeline):
    fileName = "config.yaml"
    directory = "./datafusion/" + namespace +'/'+ pipeline
    path = directory + "/" + fileName

    d = data.get("pipeline_vars_dev")
    dic = {"pipeline_vars_dev": {x: d[x] for x in d}}

    data = yaml.dump(dic, default_flow_style=False)
    
    #path.mkdir(parents=True, exist_ok=True)
    if not os.path.exists(directory):
        os.makedirs(directory, mode=755)
    
    with open(path, "w") as f:
        f.write(data)


url = "https://datafusion-gb-dev-data-tools-developer-dot-use1.datafusion.googleusercontent.com/api/v3/"
headers = {
    "Authorization": "Bearer ya29.a0ARrdaM9ppHEABuyvkIiDuhf4MrtQRgKHYLosFRHrZ11B-S3IP8x4_gTbW-W-RndE3dwNctZgTudt3LWrUpJ219FpWtP3RmNFg6nm8wadtP7L1vcusRZ1C4j-vyqI8qBzb4ojeSPz-4lF_O-UjtdJ4eHGFr7D7EKx8tj1_Xv_tiDY5JtUZ5yYHk01fuesYXi0Bs51SejaBuRPmSmuT40FKZaEsWL7B8mLrI83NHEp960RG7OKdzy07P5X86fK8PXDkYqkwbE",
    "Content-Type": "application/json",
}
r = requests.get(url, headers=headers)

data = yaml.full_load(open("execution_plan.yaml"))

namespace_name = data.get("namespace")
pipeline_name = data.get("pipeline")

# print()

# yaml.add_representer(str, represent_str)


r = requests.get(
    f"{url}namespaces/{namespace_name}/apps/{pipeline_name}", headers=headers
)

pipe = r.json()

# print(json.dumps(r.json(), indent=4, sort_keys=True))

p = {"name": "", "description": "", "artifact": "", "config": ""}
p["name"] = pipe.get("name")
p["description"] = pipe.get("description")
p["artifact"] = pipe.get("artifact")
p["config"] = json.loads(pipe.get("configuration"))
spec = json.dumps(p, sort_keys=True, indent=4)

exportPipeline(namespace_name, pipeline_name, spec)
write_config_file(data, namespace_name, pipeline_name)

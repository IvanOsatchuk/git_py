import json
import os

import requests
import yaml

def teste():
    print('hello')

def git_call(msg, branch):
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


def write_config_file(data, namespace):
    fileName = "config.yaml"
    directory = "datafusion/" + namespace
    path = directory + "/" + fileName

    d = data.get("pipeline_vars_dev")
    dic = {"pipeline_vars_dev": {x: d[x] for x in d}}

    data = yaml.dump(dic, default_flow_style=False)
    
    with open(path, "w") as f:
        f.write(data)


url = "https://datafusion-gb-dev-data-tools-developer-dot-use1.datafusion.googleusercontent.com/api/v3/"
headers = {
    "Authorization": "Bearer ya29.a0ARrdaM-hFK3P_3AGGKzk7S0nASaiQuwawJf94w2JrzejymUL8SZm9cQjmO8Mup-EaDKDoxDdVZZEgDOqlcY80Uh_sYP28BUTlIG3A60zOLEzWeyPXHYOiIwnri0VQ8qB-nLPJpHdRYDNnZ06ZbH9nfCXBGgGh_j5fwUITrSJ1xfiyGMExKHDJ62SCnTNGt4UNlObb8sC3e-IXFa9h5S6478gzeUZCCXQOIpNDa-U8hgLDDiW9jnCXytYfYoSfgID0i_HBKc",
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
write_config_file(data, namespace_name)

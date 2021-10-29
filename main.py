import json
import os

import requests
import yaml


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
    "Authorization": "Bearer ya29.a0ARrdaM-KK7aNbeY10DpJNbDHwiY-yOGaZUT86_FLQ3q9yOq8ix_TtC-usWkE50o0DOfVGu723hbBBUeqrZzPZp1ClaWXpeP-CoqwTktvztDDg6ASF8MRc1DzdQKFsjlzaRIeRZVi5otAiU85Icyqq1mVEQYFf4zXPKcJmozSznzPlk9-_n8IHR7qJEcQ_GHPYuP5VyoCnQgpyR4pgaGwYALfSDjLOG45xkHWK8MuQjRh5sQmBPWCrnPDK82Ji00Yw_qinm8",
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

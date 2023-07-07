import argparse
import yaml
import sys
import os
from jinja2 import Environment, FileSystemLoader

parser = argparse.ArgumentParser()

parser.add_argument("--config-yaml-path", "-c", required=True, help="Config YAML to run flink.")
parser.add_argument("--jinja-template-path", "-j", required=False, default="run-flink-code.py.jinja2",
                    help="Jinja2 template to use for generating job script.")

args = parser.parse_args(sys.argv[1:])

config_yaml_path = args.config_yaml_path

with open(config_yaml_path, "r") as stream:
    try:
        data = yaml.load(stream, yaml.FullLoader)

        if "query" in data:
            print("python3 run-flink-sql.py " + args.config_yaml_path)
            os.system("python3 run-flink-sql.py " + args.config_yaml_path)

        elif "code" in data:
            env = Environment(loader=FileSystemLoader('./'))
            template = env.get_template(args.jinja_template_path)
            with open("run-flink-code.py", "w") as target_file:
                target_file.write(template.render(code=data["code"]))
            print("python3 run-flink-code.py " + args.config_yaml_path)
            os.system("python3 run-flink-code.py " + args.config_yaml_path)
        else:
            raise ValueError(f"Neither query nor code were found in the provided yaml file.")

    except yaml.YAMLError as exc:
        print(exc)

import yaml


def load_config(path="config/pipeline_config.template.yaml"):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)
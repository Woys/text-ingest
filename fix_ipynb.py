import json, glob

for file in glob.glob("examples/*.ipynb"):
    with open(file, "r") as f:
        data = json.load(f)

    # Just skip long lines, there is a way to configure ruff to ignore them in notebook files, let's look at pyproject.toml

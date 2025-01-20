# /// script
# dependencies = [
#       "jinja2>=3.1.5",
#       "mergedeep>=1.3.4",
#       "tomli-w>=1.2.0",
#       "tomli>=1.2.0",
# ]
# ///

from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import mergedeep  # type: ignore
import tomli
import tomli_w

PACKAGE_DIR = Path("source")
DEVCONTAINER_DIR = Path(".devcontainer")
DOCKER_DIR = Path(".docker")


def render(template_name, **kwargs):
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template(template_name)
    return template.render(**kwargs)


def main():
    packages = Path(PACKAGE_DIR).rglob("pyproject.toml")
    package_names = []
    for pyproject_path in packages:
        with open(pyproject_path, "rb") as f:
            pyproject = tomli.load(f)
        package_name = pyproject.get("project", {}).get("name")
        if not package_name:
            raise ValueError(f"Missing project name in {pyproject_path}")

        ## Sync pyproject.toml
        pyproject_template = render("pyproject.toml.j2", package_name=package_name)
        merged = mergedeep.merge(
            {"project": pyproject.get("project", {})},
            tomli.loads(pyproject_template),
            pyproject,
            strategy=mergedeep.Strategy.REPLACE,
        )
        with open(pyproject_path, "wb") as f:
            tomli_w.dump(merged, f, multiline_strings=False)

        ## Sync devcontainer
        devcontainer_dir = DEVCONTAINER_DIR / package_name
        devcontainer_dir.mkdir(exist_ok=True)
        with open(devcontainer_dir / "devcontainer.json", "w") as f:
            f.write(render("devcontainer.json.j2", package_name=package_name))

        ## Add package name to list
        package_names.append(package_name)

    ## Sync docker-compose.yml
    docker_dir = DOCKER_DIR / "docker-compose.yml"
    docker_dir.parent.mkdir(exist_ok=True)
    with open(docker_dir, "w") as f:
        f.write(render("docker-compose.yml.j2", packages=package_names))


if __name__ == "__main__":
    main()

from setuptools import setup, find_packages

setup(
    name="opengeh-capacity-settlement",
    version=1.0,
    description="Tools for capacity settlement",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    # Make sure these packages are added to the docker container and pinned to the same versions
    install_requires=[
        "ConfigArgParse==1.7.0",
        "pyspark==3.5.1",
        "delta-spark==3.2.0",
        "python-dateutil==2.8.2",
        "azure-monitor-opentelemetry==1.6.4",
        "azure-core==1.32.0",
        "opengeh-telemetry @ git+https://git@github.com/Energinet-DataHub/opengeh-python-packages@3.0.1#subdirectory=source/telemetry",
    ],
    entry_points={
        "console_scripts": [
            "execute    = capacity_settlement.entry_point:execute",
        ]
    },
)

from setuptools import setup, find_packages

setup(
    name="opengeh-electrical-heating",
    version="1.0",
    description="Tools for electrical heating",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    # Make sure these packages are added to the docker container and pinned to the same versions
    install_requires=[
        "azure-core==1.32.0",
        "azure-monitor-opentelemetry==1.6.4",
        "delta-spark==3.2.0",
        "fire==0.7.0",
        "opengeh-telemetry @ git+https://git@github.com/Energinet-DataHub/opengeh-python-packages@2.5.1#subdirectory=source/telemetry",
        "opengeh-testcommon @ git+https://git@github.com/Energinet-DataHub/opengeh-python-packages@2.5.1#subdirectory=source/testcommon",
        "pyspark==3.5.1",
        "python-dateutil==2.8.2",
    ],
    entry_points={
        "console_scripts": [
            "execute    = electrical_heating.main:execute",
        ]
    },
)

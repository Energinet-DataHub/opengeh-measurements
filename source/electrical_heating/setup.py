﻿# Copyright 2020 Energinet DataHub A/S
#
# Licensed under the Apache License, Version 2.0 (the "License2");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from setuptools import setup, find_packages

setup(
    name="opengeh-electrical-heating",
    version=1.0,
    description="Tools for electrical heating",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    # Make sure these packages are added to the docker container and pinned to the same versions
    install_requires=[
        "ConfigArgParse==1.7.0",
        "pyspark==3.5.1",
        "delta-spark==3.2.0",
        "python-dateutil==2.8.2",
        "azure-monitor-opentelemetry==1.6.4",
        "azure-core==1.32.0",
        "opengeh-telemetry @ git+https://git@github.com/Energinet-DataHub/opengeh-python-packages@2.4.1#subdirectory=source/telemetry",
    ],
    entry_points={
        "console_scripts": [
            "execute    = electrical_heating_job.entry_point:execute",
        ]
    },
)
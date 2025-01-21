# Copyright 2020 Energinet DataHub A/S
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
# Variables defined in the infrastructure repository (https://github.com/Energinet-DataHub/dh3-infrastructure)
import os
from enum import Enum
from typing import Any


class EnvironmentVariable(Enum):
    TENANT_ID = "TENANT_ID"
    SPN_APP_ID = "SPN_APP_ID"
    SPN_APP_SECRET = "SPN_APP_SECRET"
    EVENT_HUB_NAMESPACE = "EVENT_HUB_NAMESPACE"
    EVENT_HUB_INSTANCE = "EVENT_HUB_INSTANCE"


def get_tenant_id() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.TENANT_ID)


def get_spn_app_id() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.SPN_APP_ID)


def get_spn_app_secret() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.SPN_APP_SECRET)


def get_event_hub_namespace() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.EVENT_HUB_NAMESPACE)


def get_event_hub_instance() -> str:
    return get_env_variable_or_throw(EnvironmentVariable.EVENT_HUB_INSTANCE)


def get_env_variable_or_throw(variable: EnvironmentVariable) -> Any:
    env_variable = os.getenv(variable.name)
    if env_variable is None:
        raise ValueError(f"Environment variable not found: {variable.name}")

    return env_variable

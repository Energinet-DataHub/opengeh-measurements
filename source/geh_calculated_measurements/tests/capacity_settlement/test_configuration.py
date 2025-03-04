from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


class TestConfiguration:
    __test__ = False

    def __init__(self, azure_keyvault_url: str):
        self._credential = DefaultAzureCredential()
        self._azure_keyvault_url = azure_keyvault_url

        # noinspection PyTypeChecker
        self._secret_client = SecretClient(
            vault_url=self._azure_keyvault_url,
            credential=self._credential,
        )

    @property
    def credential(self) -> DefaultAzureCredential:
        return self._credential

    def get_applicationinsights_connection_string(self) -> str:
        # This is the name of the secret in Azure Key Vault in the integration test environment
        return self._get_secret_value("AZURE-APPINSIGHTS-CONNECTIONSTRING")

    def _get_secret_value(self, secret_name: str) -> str:
        secret = self._secret_client.get_secret(secret_name)
        if not secret.value:
            raise ValueError(f"Secret {secret_name} not found in Azure Key Vault")
        return secret.value

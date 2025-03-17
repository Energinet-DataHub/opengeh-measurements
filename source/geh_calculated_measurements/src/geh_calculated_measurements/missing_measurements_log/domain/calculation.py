from geh_common.telemetry import use_span


@use_span()
def execute() -> None:
    dummy()


# Dummy function that activates logging to Azure.
# Remove this function when the real implementation is added.
@use_span()
def dummy() -> int:
    return 1 + 1

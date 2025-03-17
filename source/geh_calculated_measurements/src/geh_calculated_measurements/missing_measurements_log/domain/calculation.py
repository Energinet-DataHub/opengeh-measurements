from geh_common.telemetry import use_span


@use_span()
def execute() -> None:
    temp = 1 + 1

import core.gold.application.streams.gold_measurements_stream as gold_measurements_stream


def stream_silver_to_gold_measurements() -> None:
    gold_measurements_stream.stream_measurements_silver_to_gold()

import core.gold.application.streams.calculated_measurements_stream as calculated_measurements_stream
import core.gold.application.streams.gold_measurements_stream as gold_measurements_stream
import core.gold.application.streams.migrated_transactions_stream as migrated_transactions_stream


def stream_silver_to_gold_measurements() -> None:
    gold_measurements_stream.stream_measurements_silver_to_gold()


def stream_calculated_to_gold_measurements() -> None:
    calculated_measurements_stream.stream_measurements_calculated_to_gold()


def stream_migrated_transactions_to_gold_measurements() -> None:
    migrated_transactions_stream.stream_migrated_transactions_to_gold()

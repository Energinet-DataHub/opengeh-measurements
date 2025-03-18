class SubmittedTransactionsUnpackedColumnNames:
    orchestration_type = "orchestration_type"
    orchestration_instance_id = "orchestration_instance_id"
    metering_point_id = "metering_point_id"
    transaction_id = "transaction_id"
    transaction_creation_datetime = "transaction_creation_datetime"
    metering_point_type = "metering_point_type"
    unit = "unit"
    resolution = "resolution"
    start_datetime = "start_datetime"
    end_datetime = "end_datetime"
    points = "points"

    class Points:
        position = "points.position"
        quantity = "points.quantity"
        quality = "points.quality"

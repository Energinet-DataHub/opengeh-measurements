from geh_common.telemetry import use_span


@use_span()
def execute() -> None:
    temp = 1 + 1


# TODO Uncomment the following code snippet when repo logic is implemented.D
# @use_span()
# def execute(
#    consumption_metering_point_periods: ConsumptionMeteringPointPeriods,
#    child_metering_points: ChildMeteringPoints,
#    time_series_points: TimeSeriesPoints,
# ) -> Tuple[Cenc, Daily]:
#    cenc = calculate_cenc()
#    daily = calculate_daily(cenc, consumption_metering_point_periods, child_metering_points, time_series_points)
#    return (cenc, daily)

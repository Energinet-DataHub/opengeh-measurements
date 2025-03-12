class EphemeralColumnNames:
    """Names of calculated columns.

    `_lt` suffixes stands for local time.
    """

    base_period_limit = "base_period_limit"
    """The base period limit for the period. The base limit represents the fraction of the yearly base limit
    adjusted to how many days there are in the period compared to a whole year."""
    consumption_from_grid_metering_point_id = "consumption_from_grid_metering_point_id"
    consumption_from_grid_period_start = "consumption_from_grid_period_start"
    consumption_from_grid_period_end = "consumption_from_grid_period_end"
    consumption_from_grid_quantity = "consumption_from_grid_quantity"
    consumption_quantity = "consumption_quantity"
    cumulative_quantity = "cumulative_quantity"
    """The quantity cumulated from the first day of the period up until the current day."""
    electrical_heating_metering_point_id = "electrical_heating_metering_point_id"
    electrical_heating_period_end = "electrical_heating_period_end"
    electrical_heating_period_start = "electrical_heating_period_start"
    energy_source_metering_point_id = "energy_source_metering_point_id"
    """
    The metering point id from which to get the energy data.
    This is the net consumption metering point id if it exists, otherwise it's the consumption metering point id.
    """
    net_consumption_metering_point_id = "net_consumption_metering_point_id"
    net_consumption_period_end = "net_consumption_period_end"
    net_consumption_period_start = "net_consumption_period_start"
    observation_time_hourly = "observation_time_hourly"
    observation_time_hourly_lt = "observation_time_hourly_local_time"
    overlap_period_end_lt = "overlap_period_end"
    overlap_period_start_lt = "overlap_period_start"
    """The period of the overlap between the consumption and child metering points.
    It is needed to only calculate for the part of the period where required child metering points are available."""
    parent_net_settlement_group = "parent_net_settlement_group"
    parent_period_end = "parent_period_end"
    parent_period_start = "parent_period_start"
    """The period of the consumption metering point. It is needed to calculate limits for the period."""
    period_energy_limit = "period_energy_limit"
    period_year_lt = "period_year"
    """The type is TIMESTAMP."""
    settlement_month_datetime = "settlement_month_datetime"
    supply_to_grid_metering_point_id = "supply_to_grid_metering_point_id"
    supply_to_grid_period_start = "supply_to_grid_period_start"
    supply_to_grid_period_end = "supply_to_grid_period_end"
    supply_to_grid_quantity = "supply_to_grid_quantity"

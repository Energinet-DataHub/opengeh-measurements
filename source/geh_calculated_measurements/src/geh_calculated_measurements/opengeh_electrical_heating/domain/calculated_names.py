class CalculatedNames:
    """Names of calculated columns."""

    cumulative_quantity = "cumulative_quantity"
    """The quantity cumulated from the first day of the period up until the current day."""
    date = "date"
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
    overlap_period_end = "overlap_period_end"
    overlap_period_start = "overlap_period_start"
    parent_net_settlement_group = "parent_net_settlement_group"
    parent_period_end = "parent_period_end"
    parent_period_start = "parent_period_start"
    period_energy_limit = "period_energy_limit"
    period_year = "period_year"

from datetime import datetime, timedelta

import pyspark.sql.types as T
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize a spark session
spark = (
    SparkSession.builder.appName("geh_calculated_measurements")  # # type: ignore
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # Enable Hive support for persistence across test sessions
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
)
spark = configure_spark_with_delta_pip(spark).getOrCreate()

# Define parameters for the table generation
num_records = 500000
electrical_heating_share = 1.0
consumption_from_grid_share = 0.02
net_consumption_electrical_share = 0.02
supply_to_grid_share = 0.02

# Parent_id data frame generation --------------------------------------------------------------------------------------
## Generate parent_metering_point_id DataFrame
parent_metering_point_id_df = (
    spark.range(num_records)  # Generates values from 0 to num_records - 1
    .withColumn("parent_metering_point_id", F.expr("uuid()"))
    .select(
        F.col("id"),  # Keep the id column for sampling
        F.col("parent_metering_point_id").cast(T.StringType()),
    )
    .cache()
)


# Child_metering_point data frame generation ---------------------------------------------------------------------------
"""
Child metering points related to electrical heating.

Periods are included when
- the metering point is of type
  'supply_to_grid' 'consumption_from_grid' | 'electrical_heating' | 'net_consumption'
- the simulated data will consist of 100% relation to electrical_heating child, plus 20% relation to consumption_from_grid child
  , plus 20% relation to supply_to_grid child. 
- the metering point is coupled to a parent metering point
  Note: The same child metering point cannot be re-coupled after being uncoupled
- the child metering point physical status is connected or disconnected.
- the period does not end before 2021-01-01

Formatting is according to ADR-144 with the following constraints:
- No column may use quoted values
- All date/time values must include seconds
"""


## Generate child metering points function
def generate_child_metering_points(df, metering_type, share):
    return (
        df.sample(True, share)
        .withColumn("metering_point_id", F.expr("uuid()"))
        .withColumn("metering_point_type", F.lit(metering_type))
        .withColumn("metering_point_sub_type", F.lit("calculated"))
        .withColumn("resolution", F.lit("PT1H"))
        .withColumn("coupled_date", F.expr("date_add(current_date(), -365 - int(rand() * 730))"))
        .withColumn(
            "uncoupled_date",
            F.when(F.expr("rand() < 0.8"), None).otherwise(F.expr("date_add(coupled_date, int(rand() * 365))")),
        )
        .select(
            F.col("metering_point_id").cast(T.StringType()),
            F.col("parent_metering_point_id").cast(T.StringType()),
            F.col("metering_point_type").cast(T.StringType()),
            F.col("metering_point_sub_type").cast(T.StringType()),
            F.col("resolution").cast(T.StringType()),
            F.col("coupled_date").cast(T.TimestampType()),
            F.col("uncoupled_date").cast(T.TimestampType()),
        )
        .cache()
    )


## Generate child metering point DataFrames
child_metering_points_electrical_heating_df = generate_child_metering_points(
    parent_metering_point_id_df, "electrical_heating", electrical_heating_share
)
child_metering_points_consumption_from_grid_df = generate_child_metering_points(
    parent_metering_point_id_df, "consumption_from_grid", consumption_from_grid_share
)
child_metering_points_net_consumption_df = generate_child_metering_points(
    parent_metering_point_id_df, "net_consumption", net_consumption_electrical_share
)
child_metering_points_net_supply_to_grid_df = generate_child_metering_points(
    parent_metering_point_id_df, "supply_to_grid", supply_to_grid_share
)

## Combine all DataFrames into a single DataFrame
child_metering_points_df = (
    child_metering_points_consumption_from_grid_df.unionAll(child_metering_points_electrical_heating_df)
    .unionAll(child_metering_points_net_consumption_df)
    .unionAll(child_metering_points_net_supply_to_grid_df)
)

# Consumption_metering_point_periods data frame generation -------------------------------------------------------------
"""
- Consumption (parent) metering points related to electrical heating.
- The table works as a slowly-changing-dimension, where a new line for the (parent) metering points will occur when a status
  changes, like has_electrical_heating. The timestamps columns will indicate the timeframe where the staus of the metering 
  point is actual.

The data is periodized; the following transaction types are relevant for determining the periods:
- CHANGESUP: Leverandørskift (BRS-001)
- ENDSUPPLY: Leveranceophør (BRS-002)
- INCCHGSUP: Håndtering af fejlagtigt leverandørskift (BRS-003)
- MSTDATSBM: Fremsendelse af stamdata (BRS-006) - Skift af nettoafregningsgrupper
- LNKCHLDMP: Tilkobling af D15 til parent i nettoafregningsgruppe 2
- ULNKCHLDMP: Afkobling af D15 af parent i nettoafregningsgruppe 2
- ULNKCHLDMP: Afkobling af D14 af parent
- MOVEINES: Tilflytning - meldt til elleverandøren (BRS-009)
- MOVEOUTES: Fraflytning - meldt til elleverandøren (BRS-010)
- INCMOVEAUT: Fejlagtig flytning - Automatisk (BRS-011)
- INCMOVEMAN: Fejlagtig flytning - Manuel (BRS-011) HTX
- MDCNSEHON: Oprettelse af elvarme (BRS-015) Det bliver til BRS-041 i DH3
- MDCNSEHOFF: Fjernelse af elvarme (BRS-015) Det bliver til BRS-041 i DH3
- CHGSUPSHRT: Leverandørskift med kort varsel (BRS-043). Findes ikke i DH3
- MANCHGSUP: Tvunget leverandørskifte på målepunkt (BRS-044).
- MANCOR (HTX): Manuelt korrigering

Periods are  included when
- the metering point physical status is connected or disconnected
- the period does not end before 2021-01-01
- the electrical heating is or has been registered for the period

Formatting is according to ADR-144 with the following constraints:
- No column may use quoted values
- All date/time values must include seconds
"""

## We assume all parent metering points start with an electrical heating child metering point.
consumption_metering_points_periods_period1_df = (
    parent_metering_point_id_df.withColumn("net_settlement_group", F.when(F.rand() < 0.8, 2).otherwise(6))
    .withColumn("has_electrical_heating", F.lit(True))
    .withColumn("period_from_date", F.lit("2021-01-01").cast(T.DateType()))
    .withColumn("period_to_date", F.expr("date_add(period_from_date, cast(rand() * 365 as int))"))
    .withColumn(
        "settlement_month", F.when(F.col("net_settlement_group") == 2, F.lit(1)).otherwise(F.expr("ceil(rand() * 12)"))
    )
    .select(
        F.col("parent_metering_point_id").alias("metering_point_id"),
        F.col("has_electrical_heating"),
        F.col("net_settlement_group"),
        F.col("settlement_month"),
        F.col("period_from_date").cast(T.TimestampType()),
        F.col("period_to_date").cast(T.TimestampType()),
    )
).cache()

## Simulating metering points that uncouple from electrical heating
electrical_heating_uncouple_share = 0.2

consumption_metering_points_periods_period2_df = (
    consumption_metering_points_periods_period1_df.sample(False, electrical_heating_uncouple_share)
    .withColumn("net_settlement_group", F.lit(None).cast(T.IntegerType()))
    .withColumn("has_electrical_heating", F.lit(False))
    .withColumn("period_from_date", F.expr("date_add(period_to_date, 1)"))
    .withColumn("period_to_date", F.expr("date_add(period_from_date, cast(rand() * 365 as int))"))
    .withColumn("settlement_month", F.lit(1))
    .select(
        F.col("metering_point_id"),
        F.col("has_electrical_heating"),
        F.col("net_settlement_group"),
        F.col("settlement_month"),
        F.col("period_from_date").cast(T.TimestampType()),
        F.col("period_to_date").cast(T.TimestampType()),
    )
).cache()

## Simulating metering points that recouple to electrical heating
electrical_heating_recouple_share = 0.4

consumption_metering_points_periods_period3_df = (
    consumption_metering_points_periods_period2_df.sample(False, electrical_heating_recouple_share)
    .withColumn("net_settlement_group", F.when(F.rand() < 0.8, 2).otherwise(6))
    .withColumn("has_electrical_heating", F.lit(True))
    .withColumn("period_from_date", F.expr("date_add(period_to_date, 1)"))
    .withColumn("period_to_date", F.expr("date_add(period_from_date, cast(rand() * 365 as int))"))
    .withColumn(
        "settlement_month", F.when(F.col("net_settlement_group") == 2, F.lit(1)).otherwise(F.expr("ceil(rand() * 12)"))
    )
    .select(
        F.col("metering_point_id"),
        F.col("has_electrical_heating"),
        F.col("net_settlement_group"),
        F.col("settlement_month"),
        F.col("period_from_date").cast(T.TimestampType()),
        F.col("period_to_date").cast(T.TimestampType()),
    )
).cache()

## Combine all DataFrames into a single DataFrame
consumption_metering_points_periods_df = consumption_metering_points_periods_period1_df.unionAll(
    consumption_metering_points_periods_period2_df
).unionAll(consumption_metering_points_periods_period3_df)

# Time_series_points data frame generation -----------------------------------------------------------------------------
"""
- Time series points for electricity consumption
- The table simulates consumption data on 'consumption' metering point (parent), 'supply_to_grid' metering points,
  'consumption_from_grid' metering points, 'electrical_heating' metering points, and 'net_consumption' metering points.
- The time interval for the 'consumption' metering point is daily where the 'supply_to_grid', 'consumption_from_grid',
  'electrical_heating', and 'net_consumption' are every 15 minutes.
- The quantity for all types of metering points can vary between 0-5 in quantity (assumption)
"""

## Define the time interval
### Generate start and end date
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = today - timedelta(days=20)

### Define the time interval for simulation
time_interval_daily_df = (
    spark.range(1, (today - start_date).days + 1)  # Generates a range from 0 to the number of days
    .withColumn("datetime", F.expr(f"date_add('{start_date.strftime('%Y-%m-%d')}', cast(id as int))"))
    .select(F.col("datetime").cast(T.TimestampType()))  # Select only the date column
)

## Define the simulation for parent metering point consumption
parent_time_series_df = (
    parent_metering_point_id_df.select("parent_metering_point_id")
    .withColumnRenamed("parent_metering_point_id", "metering_point_id")
    .crossJoin(time_interval_daily_df)
    .withColumn("quantity", (F.rand() * 5).cast(T.DecimalType(18, 3)))
    .withColumnRenamed("datetime", "observation_time")
    .withColumn("metering_point_type", F.lit("consumption"))
    .select(
        F.col("metering_point_id"),
        F.col("observation_time").cast(T.TimestampType()),
        F.col("quantity"),
        F.col("metering_point_type"),
    )
)

## Define the simulation for child metering point consumption
### Define the interval dataframe of 15 minutes simulation observations
time_interval_15min_df = spark.range(96).selectExpr("id * 15 as minutes_offset")

## Define the child metering point time series lines
child_time_series_df = (
    child_metering_points_df.select("metering_point_id", "metering_point_type")
    .crossJoin(time_interval_daily_df)
    .crossJoin(time_interval_15min_df)
    .withColumn("observation_time", F.expr("timestampadd(SECOND, minutes_offset * 60, datetime)"))
    .withColumn("quantity", (F.rand() * 5).cast(T.DecimalType(18, 3)))
    .select(
        F.col("metering_point_id"),
        F.col("observation_time").cast(T.TimestampType()),
        F.col("quantity"),
        F.col("metering_point_type"),
    )
)

## Union the two timeseries tables together
time_series_df = parent_time_series_df.union(child_time_series_df)


# Writing down the tables in unit catalog ------------------------------------------------------------------------------
catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
ctl_shres_d_we_catalog = [catalog for catalog in catalogs if catalog.startswith("ctl_shres_d_we")]

(
    child_metering_points_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{ctl_shres_d_we_catalog}.test.child_metering_points")
)

(
    consumption_metering_points_periods_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{ctl_shres_d_we_catalog}.test.consumption_metering_points_periods")
)

(
    time_series_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{ctl_shres_d_we_catalog}.test.time_series_points")
)

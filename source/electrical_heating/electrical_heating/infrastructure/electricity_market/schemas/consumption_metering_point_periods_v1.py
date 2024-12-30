import pyspark.sql.types as t

nullable = True

# Consumption (parent) metering points related to electrical heating.
#
# The data is periodized; the following transaction types are relevant for determining the periods:
# - CHANGESUP: Leverandørskift (BRS-001)
# - ENDSUPPLY: Leveranceophør (BRS-002)
# - INCCHGSUP: Håndtering af fejlagtigt leverandørskift (BRS-003)
# - MSTDATSBM: Fremsendelse af stamdata (BRS-006) - Skift af nettoafregningsgrupper
# - LNKCHLDMP: Tilkobling af D15 til parent i nettoafregningsgruppe 2
# - ULNKCHLDMP: Afkobling af D15 af parent i nettoafregningsgruppe 2
# - CLSDWNMP: Nedlæggelse af målepunkt (BRS-007)
# - MOVEINES: Tilflytning - meldt til elleverandøren (BRS-009)
# - MOVEOUTES: Fraflytning - meldt til elleverandøren (BRS-010)
# - INCMOVEAUT: Fejlagtig flytning - Automatisk (BRS-011)
# - INCMOVEMAN: Fejlagtig flytning - Manuel (BRS-011) HTX
# - MDCNSEHON: Oprettelse af elvarme (BRS-015) Det bliver til BRS-041 i DH3
# - MDCNSEHOFF: Fjernelse af elvarme (BRS-015) Det bliver til BRS-041 i DH3
# - CHGSUPSHRT: Leverandørskift med kort varsel (BRS-043). Findes ikke i DH3
# - MANCHGSUP: Tvunget leverandørskifte på målepunkt (BRS-044).
# - MANCOR (HTX): Manuelt korrigering
#
# Periods are  included when
# - the metering point physical status is connected or disconnected
# - the period does not end before 2021-01-01
# - the electrical heating is or has been registered for the period
consumption_metering_point_periods_v1 = t.StructType(
    [
        #
        # GSRN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # States whether the metering point has electrical heating in the period
        # true:  The consumption metering has electrical heating in the stated period
        # false: The consumption metering point was previously marked as having electrical
        #        heating in the stated period, but this has been corrected
        t.StructField("has_electrical_heating", t.BooleanType(), not nullable),
        #
        # 2 | 3 | 4 | 5 | 6 | 99 | NULL
        t.StructField("net_settlement_group", t.IntegerType(), not nullable),
        #
        # Settlement month is 1st of January for all consumption with electrical heating except for
        # net settlement group 6, where the date is the scheduled meter reading date.
        # The number of the month. 1 is January, 12 is December.
        # For all but settlement group 6 the month is January.
        t.StructField(
            "settlement_month",
            t.IntegerType(),
            nullable,
        ),
        #
        # See the description of periodization of data above.
        # UTC time
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        #
        # See the description of periodization of data above.
        # UTC time
        t.StructField("period_to_date", t.TimestampType(), nullable),
    ]
)
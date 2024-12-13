import pyspark.sql.types as t

nullable = True

# Includes all periods of consumption metering points where electrical heating is registered.
# Metering point periods where the electrical heating flag is not set must be omitted from the data set.
#
# A period is created whenever any of the following transaction types are registered:
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
consumption_metering_point_periods_v1 = t.StructType(
    [
        #
        # GSRN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
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
        # UTC time
        t.StructField("period_from_date", t.TimestampType(), not nullable),
        #
        # UTC time
        t.StructField("period_to_date", t.TimestampType(), nullable),
    ]
)
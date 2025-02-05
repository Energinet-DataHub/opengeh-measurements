# Gold Layer

This python module handles streaming from the silver layer, to the gold layer.

## Gold Schema

See the [gold measurements](domain/schemas/gold_measurements.py) schema for more information, on the columns and data types used in the gold layer.

## Gold Transformations

The transformation from the Silver layer to the Gold layer consists of the following steps:

- **Exploding silver `points` column**: Each point in the `points` array is expanded into its own row, flattening the data.

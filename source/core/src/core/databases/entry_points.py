import core.databases.migrations.migrations_runner as migrations_runner
import core.databases.optimization.optimize_table as optimization
from core.databases.optimization.optimize_args import OptimizeArgs


def migrate() -> None:
    migrations_runner.migrate()


def optimize_table() -> None:
    optimize_args = OptimizeArgs()
    optimization.optimize_table(
        optimize_args.database,
        optimize_args.table,
    )

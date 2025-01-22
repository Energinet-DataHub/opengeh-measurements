class DeltaSilverRepositoryConfig:
    def __init__(self, silver_database: str):
        self.silver_database = silver_database

    def validate(self):
        if not self.silver_database:
            raise ValueError("silver_database must not be empty")

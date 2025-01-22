class DeltaGoldRepositoryConfig:
    def __init__(self, gold_database: str):
        self.gold_database = gold_database

    def validate(self):
        if not self.gold_database:
            raise ValueError("gold_database must not be empty")

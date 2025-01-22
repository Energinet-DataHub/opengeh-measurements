class DeltaGoldWriterConfig:
    def __init__(self, gold_path: str, query_name: str, checkpoint_location: str):
        self.gold_path = gold_path
        self.query_name = query_name
        self.checkpoint_location = checkpoint_location

    def validate(self):
        if not self.gold_path:
            raise ValueError("gold_path must not be empty")
        if not self.query_name:
            raise ValueError("query_name must not be empty")
        if not self.checkpoint_location:
            raise ValueError("checkpoint_location must not be empty")

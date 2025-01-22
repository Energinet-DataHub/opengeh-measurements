class DeltaSilverReaderConfig:
    def __init__(self, full_silver_table_name: str, read_options: dict[str, str]):
        self.full_silver_table_name = full_silver_table_name
        self.read_options = read_options

    def validate(self):
        if not self.full_silver_table_name:
            raise ValueError("full_silver_table_name must not be empty")
        if not isinstance(self.read_options, dict):
            raise ValueError("read_options must be a dictionary")

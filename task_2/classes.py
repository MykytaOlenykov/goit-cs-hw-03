class ModelValidateError(Exception):
    def __init__(self, error_message) -> None:
        super().__init__()
        self.message = error_message

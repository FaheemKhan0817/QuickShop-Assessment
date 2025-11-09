class ValidationError(Exception):
    """Something's wrong with the input data — bad column, wrong type."""
    pass


class ETLError(Exception):
    """General ETL problem — file missing, write failed, etc."""
    pass
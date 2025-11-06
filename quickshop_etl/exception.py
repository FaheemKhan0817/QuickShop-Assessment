class ValidationError(Exception):
    """Raised when input CSV validation fails."""
    pass

class ETLError(Exception):
    """Generic ETL exception."""
    pass
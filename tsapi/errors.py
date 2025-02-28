
class TsApiError(Exception):
    """Base class for all TsApi errors."""
    pass


class TsApiDataError(TsApiError):
    """Raised when there is an error in the data returned by the API."""
    pass


class TsApiNoTimestampError(TsApiDataError):
    """Raised when there is no timestamp column in the data."""
    pass


class SoftdeskError(Exception):
    """Base class for Softdesk integration failures."""


class SoftdeskAPIError(SoftdeskError):
    """HTTP or contract errors when calling Softdesk."""


class SoftdeskRateLimitError(SoftdeskAPIError):
    """Softdesk or edge proxy returned HTTP 429."""

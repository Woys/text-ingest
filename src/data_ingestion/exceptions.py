"""Project-specific exception hierarchy."""


class IngestionError(Exception):
    """Base exception for the project."""


class ConfigurationError(IngestionError):
    """Raised when configuration is invalid or unsupported."""


class FetcherError(IngestionError):
    """Raised when a fetcher cannot retrieve or parse data."""


class QuotaExceededError(FetcherError):
    """Raised before making a request that would exceed a configured quota."""


class SinkError(IngestionError):
    """Raised when a sink cannot persist output."""


class PipelineError(IngestionError):
    """Raised when the pipeline fails to run successfully."""

try:
    from importlib.metadata import PackageNotFoundError, version

    __version__: str = version("massive-data-ingestion")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0+unknown"

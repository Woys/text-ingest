try:
    from importlib.metadata import PackageNotFoundError, version

    __version__: str = version("text-ingest")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0+unknown"

"""Fetcher sub-package — importing this module auto-registers all built-in fetchers."""

from .base import BaseFetcher
from .crossref import CrossRefFetcher
from .federal_register import FederalRegisterFetcher
from .hackernews import HackerNewsFetcher
from .newsapi import NewsApiFetcher
from .openalex import OpenAlexFetcher
from .website import WebsiteFetcher
from .website_html import WebsiteHtmlFetcher

__all__ = [
    "BaseFetcher",
    "CrossRefFetcher",
    "FederalRegisterFetcher",
    "HackerNewsFetcher",
    "NewsApiFetcher",
    "OpenAlexFetcher",
    "WebsiteFetcher",
    "WebsiteHtmlFetcher",
]

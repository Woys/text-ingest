"""Fetcher sub-package — importing this module auto-registers all built-in fetchers."""

from .base import BaseFetcher
from .crossref import CrossRefFetcher
from .edgar import EdgarFetcher
from .federal_register import FederalRegisterFetcher
from .github import GitHubFetcher
from .googlenews import GoogleNewsFetcher
from .guardian import GuardianFetcher
from .hackernews import HackerNewsFetcher
from .newsapi import NewsApiFetcher
from .openalex import OpenAlexFetcher
from .openlibrary import OpenLibraryFetcher
from .reddit import RedditFetcher
from .stackexchange import StackExchangeFetcher
from .website import WebsiteFetcher
from .website_html import WebsiteHtmlFetcher
from .wikipedia import WikipediaFetcher

__all__ = [
    "BaseFetcher",
    "CrossRefFetcher",
    "EdgarFetcher",
    "FederalRegisterFetcher",
    "GitHubFetcher",
    "GoogleNewsFetcher",
    "GuardianFetcher",
    "HackerNewsFetcher",
    "NewsApiFetcher",
    "OpenAlexFetcher",
    "OpenLibraryFetcher",
    "RedditFetcher",
    "StackExchangeFetcher",
    "WebsiteFetcher",
    "WebsiteHtmlFetcher",
    "WikipediaFetcher",
]

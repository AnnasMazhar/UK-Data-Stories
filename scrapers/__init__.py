"""Scrapers package."""

from scrapers.base import BaseScraper, AsyncBaseScraper
from scrapers.ons_api import ONSScraper
from scrapers.data_gov_uk import DataGovUkScraper
from scrapers.ckan_gov_uk import CkanGovUkScraper

__all__ = ["BaseScraper", "AsyncBaseScraper", "ONSScraper", "DataGovUkScraper", "CkanGovUkScraper"]

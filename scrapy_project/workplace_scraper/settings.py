from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from config.settings import get_settings

_settings = get_settings()

BOT_NAME = "workplace_scraper"
SPIDER_MODULES = ["workplace_scraper.spiders"]
NEWSPIDER_MODULE = "workplace_scraper.spiders"

USER_AGENT = _settings.scrape_user_agent
# The assignment requires /en/cases/ pages, which robots.txt disallows.
ROBOTSTXT_OBEY = False
CONCURRENT_REQUESTS = _settings.scrape_concurrency
CONCURRENT_REQUESTS_PER_DOMAIN = min(8, _settings.scrape_concurrency)
DOWNLOAD_DELAY = _settings.scrape_delay
RANDOMIZE_DOWNLOAD_DELAY = True
RETRY_TIMES = _settings.scrape_retry_times
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = _settings.scrape_delay
AUTOTHROTTLE_MAX_DELAY = 30.0
DOWNLOAD_TIMEOUT = 30
LOG_LEVEL = "INFO"

ITEM_PIPELINES: dict[str, int] = {
    "workplace_scraper.pipelines.LandingPipeline": 300,
}
DOWNLOADER_MIDDLEWARES: dict[str, int] = {
    # Before RetryMiddleware (550) so throttling is logged before the retry decision.
    "workplace_scraper.middlewares.RateLimitLoggerMiddleware": 543,
}

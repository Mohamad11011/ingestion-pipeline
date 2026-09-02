from __future__ import annotations

import logging

from observability.structured import log_event

logger = logging.getLogger("workplace.throttle")


class RateLimitLoggerMiddleware:
    """Makes 429/503 backoff visible in the JSON logs; Retry/AutoThrottle do the waiting."""

    def process_response(self, request, response, spider):
        if response.status in (429, 503):
            log_event(
                logger,
                "rate_limited",
                level=logging.WARNING,
                url=request.url,
                error_code=response.status,
                retry_after=response.headers.get("Retry-After", b"").decode() or None,
            )
        return response

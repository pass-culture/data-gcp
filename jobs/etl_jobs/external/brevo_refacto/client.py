import asyncio
import time
import logging
from typing import Dict, Optional
from dataclasses import dataclass
from datetime import datetime

import aiohttp
from aiohttp import ClientError
from config import Config

logger = logging.getLogger(__name__)

@dataclass
class TokenBucket:
    """Token Bucket Algorithm for rate limiting"""
    capacity: float
    tokens: float
    refill_rate: float
    last_refill: float
    
    def consume(self, tokens: int = 1) -> float:
        """Try to consume tokens. Returns wait time if not enough tokens."""
        now = time.time()
        
        # Refill tokens based on time passed
        time_passed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + time_passed * self.refill_rate)
        self.last_refill = now
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return 0  # No wait needed
        
        # Calculate how long to wait for enough tokens
        tokens_needed = tokens - self.tokens
        wait_time = tokens_needed / self.refill_rate
        return wait_time

class AsyncBrevoClient:
    """Async client for Brevo API with built-in rate limiting."""
    
    def __init__(self, config: Config):
        self.api_key = config.api_key
        self.base_url = config.api_base_url
        
        # Use all values from config
        self.semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        
        self.rate_limiter = TokenBucket(
            capacity=config.requests_per_hour,
            tokens=config.requests_per_hour,
            refill_rate=config.requests_per_hour / 3600,
            last_refill=time.time()
        )
        
        # Global rate limit pause - when we hit 429, pause ALL requests
        self.rate_limit_until = 0
        self.rate_limit_lock = asyncio.Lock()
        
        # Request counter for logging
        self.request_count = 0
        self.error_count = 0
        
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        """Context manager entry - creates the HTTP session"""
        self.session = aiohttp.ClientSession(
            headers={"api-key": self.api_key}
        )
        logger.info(f"Brevo client initialized. Rate limit: {self.rate_limiter.capacity} requests/hour")
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures session is closed"""
        if self.session:
            await self.session.close()
        logger.info(f"Brevo client closed. Total requests: {self.request_count}, Errors: {self.error_count}")
    
    async def wait_for_global_rate_limit(self):
        """Check and wait for global rate limit"""
        async with self.rate_limit_lock:
            if self.rate_limit_until > time.time():
                wait_time = self.rate_limit_until - time.time()
                logger.info(f"Global rate limit active. Waiting {wait_time:.2f}s...")
                await asyncio.sleep(wait_time)
    
    async def request(self, method: str, endpoint: str, **kwargs) -> Dict:
        """Make a rate-limited request to Brevo API."""
        # First check global rate limit
        await self.wait_for_global_rate_limit()
        
        # Then check token bucket
        wait_time = self.rate_limiter.consume()
        if wait_time > 0:
            logger.debug(f"Token bucket limit. Waiting {wait_time:.2f}s...")
            await asyncio.sleep(wait_time)
        
        # Semaphore ensures we don't overwhelm the server
        async with self.semaphore:
            # Retry logic with exponential backoff
            for attempt in range(3):
                try:
                    self.request_count += 1
                    
                    # Log every 50th request to show progress
                    if self.request_count % 50 == 0:
                        logger.info(f"Progress: {self.request_count} requests made, {self.rate_limiter.tokens:.0f} tokens remaining")
                    
                    async with self.session.request(
                        method, 
                        f"{self.base_url}/{endpoint}",
                        **kwargs
                    ) as response:
                        if response.status == 429:  # Rate limit hit
                            # Get actual retry time from header
                            retry_after = int(response.headers.get('Retry-After', 60))
                            
                            # Read the response body for more info
                            try:
                                error_data = await response.json()
                                logger.warning(f"Rate limit 429: {error_data.get('message', 'No message')}. Retry after {retry_after}s")
                            except:
                                logger.warning(f"Rate limit 429. Retry after {retry_after}s")
                            
                            # Set global rate limit
                            async with self.rate_limit_lock:
                                new_limit_time = time.time() + retry_after
                                if new_limit_time > self.rate_limit_until:
                                    self.rate_limit_until = new_limit_time
                                    logger.info(f"Setting global rate limit until {datetime.fromtimestamp(self.rate_limit_until).strftime('%H:%M:%S')}")
                            
                            # Wait and retry
                            await asyncio.sleep(retry_after)
                            continue
                            
                        response.raise_for_status()
                        return await response.json()
                        
                except aiohttp.ClientError as e:
                    self.error_count += 1
                    if attempt == 2:  # Last attempt
                        logger.error(f"Final attempt failed for {endpoint}: {e}")
                        raise
                    
                    # Exponential backoff: 1s, 2s, 4s...
                    wait = 2 ** attempt
                    logger.warning(f"Request failed (attempt {attempt + 1}/3), retrying in {wait}s: {e}")
                    await asyncio.sleep(wait)
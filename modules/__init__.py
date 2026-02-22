import os
import gc
import ctypes
import time
import aiohttp
from mcp.server.fastmcp import FastMCP # type: ignore
from plexapi.server import PlexServer # type: ignore
from plexapi.myplex import MyPlexAccount # type: ignore

# Environment initialization is handled by plex_mcp_server.py

# Initialize FastMCP server
mcp = FastMCP("plex-server")

# Global variables for Plex connection
plex_url = os.environ.get("PLEX_URL", "")
plex_token = os.environ.get("PLEX_TOKEN", "")
server = None
last_connection_time = 0
CONNECTION_TIMEOUT = 30  # seconds
SESSION_TIMEOUT = 60 * 30  # 30 minutes

def connect_to_plex() -> PlexServer:
    """Connect to Plex server, reusing a cached singleton instance.

    Returns a PlexServer instance. Reconnects only when the session has
    expired (SESSION_TIMEOUT) or was never created. Avoids per-call API
    verification to reduce overhead and memory churn.
    """
    global server, last_connection_time
    current_time = time.time()

    # Reuse existing connection if still within timeout window
    if server is not None and (current_time - last_connection_time) < SESSION_TIMEOUT:
        return server

    # Create a new connection (or reconnect after timeout)
    server = None
    max_retries = 3
    retry_delay = 2  # seconds

    for attempt in range(max_retries):
        try:
            if not plex_url or not plex_token:
                raise ValueError("PLEX_URL and PLEX_TOKEN are required")

            server = PlexServer(plex_url, plex_token, timeout=CONNECTION_TIMEOUT)
            last_connection_time = current_time
            return server

        except Exception as e:
            if attempt == max_retries - 1:
                raise ValueError(f"Failed to connect to Plex after {max_retries} attempts: {str(e)}")
            time.sleep(retry_delay)

    raise ValueError("Failed to connect to Plex server")


# Shared aiohttp session — avoids per-request connection pool and SSL context
# allocation. Lazy-initialized on first use.
_http_session: aiohttp.ClientSession | None = None


async def get_http_session() -> aiohttp.ClientSession:
    """Return a shared aiohttp.ClientSession, creating one if needed."""
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()
    return _http_session


def force_release_memory():
    """Force CPython + glibc to return freed pages to the kernel."""
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass

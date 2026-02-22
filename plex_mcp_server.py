import argparse
import os
from dotenv import load_dotenv # type: ignore

def init_environment():
    """Load environment variables from standard locations."""
    loaded = load_dotenv()
    config_env_file = os.path.expanduser("~/.config/plex-mcp-server/.env")
    if os.path.exists(config_env_file):
        if load_dotenv(config_env_file):
            loaded = True
    return loaded

# Load environment before any other modules are imported
env_loaded = init_environment()

# Import the main mcp instance from modules
import modules
from modules import mcp, connect_to_plex

# Import all tools to ensure they are registered with MCP
from modules.library import *  # noqa: F401,F403
from modules.user import *  # noqa: F401,F403
from modules.sessions import *  # noqa: F401,F403
from modules.server import *  # noqa: F401,F403
from modules.playlist import *  # noqa: F401,F403
from modules.collection import *  # noqa: F401,F403
from modules.media import *  # noqa: F401,F403
from modules.client import *  # noqa: F401,F403

def main():
    """Main entry point for the Plex MCP Server."""
    parser = argparse.ArgumentParser(description='Run Plex MCP Server')
    parser.add_argument('--transport', choices=['stdio', 'streamable-http'],
                        default='stdio', help='Transport method')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8000, help='Port to listen on')
    parser.add_argument('--plex-url', default=os.environ.get('PLEX_URL'),
                        help='Plex Server URL (default: PLEX_URL env var)')
    parser.add_argument('--plex-token', default=os.environ.get('PLEX_TOKEN'),
                        help='Plex Auth Token (default: PLEX_TOKEN env var)')
    args = parser.parse_args()

    modules.plex_url = args.plex_url
    modules.plex_token = args.plex_token
    if args.plex_url:
        os.environ['PLEX_URL'] = args.plex_url
    if args.plex_token:
        os.environ['PLEX_TOKEN'] = args.plex_token

    print(f"Starting Plex MCP Server with {args.transport} transport...")

    if args.transport == 'streamable-http':
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        mcp.settings.streamable_http_path = '/'
        mcp.settings.stateless_http = True
        # Reset DNS rebinding protection — FastMCP auto-enables it for the
        # default host (127.0.0.1).  In our setup auth-proxy handles security
        # and forwards requests with Host: <container_name>:8000.
        mcp.settings.transport_security = None
        mcp.run(transport='streamable-http')
    else:
        mcp.run(transport='stdio')

if __name__ == "__main__":
    main()

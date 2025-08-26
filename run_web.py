#!/usr/bin/env python
"""
Main script to run the Aegis web interfaces.

This starts the Flask application with all interfaces:
- Chat interface for interacting with the Aegis model
- Monitoring dashboard for viewing process logs
- Database viewer for exploring PostgreSQL tables
"""

from interfaces.web import app

if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Run Aegis web interfaces")
    parser.add_argument(
        "--host", 
        default=os.getenv("SERVER_HOST", "127.0.0.1"), 
        help="Host to run the server on (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=int(os.getenv("SERVER_PORT", "5000")), 
        help="Port to run the server on (default: 5000)"
    )
    parser.add_argument(
        "--debug", 
        action="store_true", 
        default=os.getenv("DEBUG", "false").lower() == "true",
        help="Run in debug mode"
    )

    args = parser.parse_args()

    print(f"Starting Aegis interfaces server on http://{args.host}:{args.port}")
    print("Available interfaces:")
    print(f"  - Chat:       http://{args.host}:{args.port}/")
    print(f"  - Monitoring: http://{args.host}:{args.port}/monitoring")
    print(f"  - Database:   http://{args.host}:{args.port}/database")

    app.run(host=args.host, port=args.port, debug=args.debug)
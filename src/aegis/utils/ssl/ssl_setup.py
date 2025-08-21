"""
SSL configuration module.

This module handles SSL certificate loading based on environment configuration.
"""

import os
from typing import Dict, Optional, Union

from aegis.utils.logging import get_logger
from aegis.utils.settings import config


def setup_ssl() -> Dict[str, Union[bool, Optional[str]]]:
    """
    Setup SSL configuration based on environment variables.

    Checks SSL_VERIFY and SSL_CERT_PATH environment variables and returns
    a consistent output schema for both verify and non-verify scenarios.

    Returns:
        Dictionary with SSL configuration:
        - "verify": bool - Whether to verify SSL
        - "cert_path": str or None - Path to certificate file if verify is True

        # Returns: {"verify": False, "cert_path": None}  # When SSL_VERIFY=false
        # Returns: {"verify": True, "cert_path": "/path/to/cert.cer"}  # With cert
        # Returns: {"verify": True, "cert_path": None}  # System certs

    Raises:
        FileNotFoundError: If SSL_VERIFY=true but cert file doesn't exist.
    """
    logger = get_logger()

    # Check if SSL verification is enabled
    if not config.ssl_verify:
        logger.debug("SSL verification disabled")
        return {"verify": False, "cert_path": None}

    # SSL verification is enabled
    cert_path = config.ssl_cert_path

    if cert_path:
        # Expand user path if needed
        cert_path = os.path.expanduser(cert_path)

        # Check if certificate file exists
        if not os.path.exists(cert_path):
            error_msg = f"SSL certificate file not found: {cert_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        logger.info("SSL verification enabled with certificate", cert_path=cert_path)
    else:
        logger.info("SSL verification enabled with system certificates")
        cert_path = None

    return {"verify": True, "cert_path": cert_path}

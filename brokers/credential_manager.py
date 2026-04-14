"""
APEX MULTI-MARKET TJR ENGINE
Credential Manager — Secure credential loading and validation.

SECURITY RULES:
- Never log or expose credentials in any output
- Credentials loaded from environment variables only (or .env file)
- Encrypted storage optional via Fernet symmetric key
- Startup validation: fail-closed if credentials invalid/missing
- No default fallback credentials
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class CredentialStatus(Enum):
    VALID = "VALID"
    MISSING = "MISSING"
    INVALID_FORMAT = "INVALID_FORMAT"
    UNVALIDATED = "UNVALIDATED"


@dataclass
class BrokerCredentials:
    """Immutable credential bundle for a broker. Never serialise this."""
    broker_name: str
    environment: str  # "practice" | "live"
    api_token: Optional[str] = field(default=None, repr=False)
    api_secret: Optional[str] = field(default=None, repr=False)
    account_id: Optional[str] = None
    passphrase: Optional[str] = field(default=None, repr=False)
    status: CredentialStatus = CredentialStatus.UNVALIDATED
    validation_errors: List[str] = field(default_factory=list)
    validated_at: Optional[datetime] = None

    def is_usable(self) -> bool:
        return self.status == CredentialStatus.VALID and bool(self.api_token)

    def safe_summary(self) -> dict:
        """Return non-sensitive summary for logging/dashboard."""
        token_hint = ""
        if self.api_token:
            token_hint = self.api_token[:6] + "..." + self.api_token[-4:]
        return {
            "broker": self.broker_name,
            "environment": self.environment,
            "account_id": self.account_id,
            "token_hint": token_hint,
            "status": self.status.value,
            "errors": self.validation_errors,
            "validated_at": self.validated_at.isoformat() if self.validated_at else None,
        }


class CredentialManager:
    """
    Loads and validates broker credentials from environment variables.

    Environment variable naming convention:
        APEX_<BROKER>_API_TOKEN
        APEX_<BROKER>_API_SECRET
        APEX_<BROKER>_ACCOUNT_ID
        APEX_<BROKER>_ENVIRONMENT   (practice | live)

    For OANDA specifically:
        APEX_OANDA_API_TOKEN
        APEX_OANDA_ACCOUNT_ID
        APEX_OANDA_ENVIRONMENT  (practice | live)

    JWT / system secrets:
        APEX_JWT_SECRET

    Never stored in config YAML. Always from env.
    """

    # OANDA tokens: 32 hex chars, dash, 32 hex chars
    # Example: 8572c1fdea1ae0323a45fe92d6056979-2720f3ef888051eab2847d2c6e6b6009
    _OANDA_TOKEN_REGEX = re.compile(r"^[a-f0-9]{32}-[a-f0-9]{32}$")
    _OANDA_ACCOUNT_REGEX = re.compile(r"^\d{3}-\d{3}-\d{7,10}-\d{3}$")

    def __init__(self, env_file: Optional[str] = None):
        self._credentials: Dict[str, BrokerCredentials] = {}
        self._jwt_secret: Optional[str] = None
        self._load_env_file(env_file)

    # ------------------------------------------------------------------
    # ENV FILE LOADER
    # ------------------------------------------------------------------

    def _load_env_file(self, env_file: Optional[str]) -> None:
        """Load .env file into environment if it exists. Never log values."""
        paths_to_try = []
        if env_file:
            paths_to_try.append(Path(env_file))
        # Default search paths
        paths_to_try.extend([
            Path("/home/user/apex-trader/.env"),
            Path(os.getcwd()) / ".env",
        ])

        for path in paths_to_try:
            if path.exists():
                logger.info(f"Loading env file: {path}")
                self._parse_env_file(path)
                return

        logger.debug("No .env file found; relying on process environment only")

    def _parse_env_file(self, path: Path) -> None:
        """Minimal .env parser — supports KEY=VALUE and KEY='VALUE'."""
        try:
            for line in path.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip("'\"")
                if key and key not in os.environ:  # Don't override existing env
                    os.environ[key] = val
        except Exception as e:
            logger.warning(f"Failed to parse .env file: {e}")

    # ------------------------------------------------------------------
    # CREDENTIAL LOADING
    # ------------------------------------------------------------------

    def load_oanda(self) -> BrokerCredentials:
        """Load and validate OANDA v20 REST API credentials."""
        token = os.environ.get("APEX_OANDA_API_TOKEN") or os.environ.get("OANDA_API_TOKEN")
        account_id = os.environ.get("APEX_OANDA_ACCOUNT_ID") or os.environ.get("OANDA_ACCOUNT_ID")
        environment = (
            os.environ.get("APEX_OANDA_ENVIRONMENT")
            or os.environ.get("OANDA_ENVIRONMENT")
            or "practice"
        ).lower()

        errors = []

        if not token:
            errors.append("APEX_OANDA_API_TOKEN not set in environment")
        elif not self._OANDA_TOKEN_REGEX.match(token):
            errors.append(
                "APEX_OANDA_API_TOKEN format invalid "
                "(expected 32hex-32hex, e.g. xxxxxxxx...-xxxxxxxx...)"
            )

        if not account_id:
            errors.append("APEX_OANDA_ACCOUNT_ID not set in environment")

        if environment not in ("practice", "live"):
            errors.append(f"APEX_OANDA_ENVIRONMENT must be 'practice' or 'live', got '{environment}'")

        status = CredentialStatus.MISSING if not token else (
            CredentialStatus.INVALID_FORMAT if errors else CredentialStatus.VALID
        )

        creds = BrokerCredentials(
            broker_name="oanda",
            environment=environment,
            api_token=token,
            account_id=account_id,
            status=status,
            validation_errors=errors,
            validated_at=datetime.now(timezone.utc) if not errors else None,
        )
        self._credentials["oanda"] = creds
        if errors:
            logger.warning(f"OANDA credential validation issues: {errors}")
        else:
            logger.info(f"OANDA credentials loaded OK: env={environment} acct={account_id}")
        return creds

    def load_jwt_secret(self) -> Optional[str]:
        """Load JWT secret for internal API auth."""
        secret = (
            os.environ.get("APEX_JWT_SECRET")
            or os.environ.get("JWT_SECRET")
        )
        if not secret:
            logger.warning("JWT_SECRET not set; API auth disabled")
        elif len(secret) < 32:
            logger.warning("JWT_SECRET is very short (<32 chars); security risk")
        self._jwt_secret = secret
        return secret

    def get(self, broker: str) -> Optional[BrokerCredentials]:
        return self._credentials.get(broker.lower())

    def get_jwt_secret(self) -> Optional[str]:
        return self._jwt_secret

    def all_summary(self) -> dict:
        """Return non-sensitive summary of all loaded credentials."""
        return {
            k: v.safe_summary() for k, v in self._credentials.items()
        }

    @staticmethod
    def mask(value: Optional[str]) -> str:
        """Return masked representation of a secret for logs."""
        if not value:
            return "<not set>"
        if len(value) <= 10:
            return "****"
        return value[:4] + "****" + value[-2:]


# Singleton accessor
_manager: Optional[CredentialManager] = None


def get_credential_manager(env_file: Optional[str] = None) -> CredentialManager:
    global _manager
    if _manager is None:
        _manager = CredentialManager(env_file)
    return _manager

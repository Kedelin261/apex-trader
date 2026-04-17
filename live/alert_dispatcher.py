"""
APEX MULTI-MARKET TJR ENGINE
Alert Dispatcher — Phase 7: Event-driven operator alerting.

ALERT TYPES:
  BROKER_CONNECTED       — broker authenticated
  BROKER_DISCONNECTED    — broker lost connection
  TRADE_OPENED           — new trade filled
  TRADE_CLOSED           — position closed
  STOP_MOVED             — SL moved (BE or trail)
  RISK_LIMIT_HIT         — risk governor blocked new entry
  SYSTEM_BLOCKED         — kill switch / ESM blocked
  LOOP_FAILURE           — scheduler loop died
  RESTART_DETECTED       — system restarted
  RECONCILIATION_MISMATCH— engine ↔ broker state diverged
  REPEATED_REJECTS       — broker rejecting multiple orders
  STALE_DATA             — market data feed stale
  DRAWDOWN_WARNING       — drawdown approaching limit
  DAILY_SUMMARY          — end-of-day performance summary

DELIVERY CHANNELS:
  - Email (SMTP)
  - Telegram bot
  - Discord webhook
  - Console log (always active)

Configuration in config.autonomy.alerting (see system_config.yaml).
Channels enabled/disabled from config. No hardcoded tokens.
"""

from __future__ import annotations

import json
import logging
import smtplib
import threading
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ALERT TYPES
# ---------------------------------------------------------------------------

ALERT_LEVELS: Dict[str, str] = {
    "BROKER_CONNECTED":       "INFO",
    "BROKER_DISCONNECTED":    "ERROR",
    "TRADE_OPENED":           "INFO",
    "TRADE_CLOSED":           "INFO",
    "STOP_MOVED":             "INFO",
    "RISK_LIMIT_HIT":         "WARNING",
    "SYSTEM_BLOCKED":         "CRITICAL",
    "LOOP_FAILURE":           "ERROR",
    "RESTART_DETECTED":       "WARNING",
    "RECONCILIATION_MISMATCH":"ERROR",
    "REPEATED_REJECTS":       "ERROR",
    "STALE_DATA":             "ERROR",
    "DRAWDOWN_WARNING":       "WARNING",
    "DAILY_SUMMARY":          "INFO",
}


@dataclass
class AlertEvent:
    alert_type: str
    message: str
    details: dict
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    level: str = "INFO"
    channel_results: Dict[str, bool] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "alert_type": self.alert_type,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp,
            "level": self.level,
            "channel_results": self.channel_results,
        }

    def format_text(self) -> str:
        lines = [
            f"[{self.level}] {self.alert_type}",
            f"Time: {self.timestamp}",
            f"Message: {self.message}",
        ]
        if self.details:
            lines.append("Details:")
            for k, v in self.details.items():
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CHANNEL BASE
# ---------------------------------------------------------------------------

class AlertChannel:
    """Base class for all alert delivery channels."""
    name: str = "base"

    def send(self, event: AlertEvent) -> bool:
        raise NotImplementedError

    def is_configured(self) -> bool:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# EMAIL CHANNEL
# ---------------------------------------------------------------------------

class EmailAlertChannel(AlertChannel):
    """SMTP email alert channel."""
    name = "email"

    def __init__(self, cfg: dict):
        self._host = cfg.get("smtp_host", "")
        self._port = int(cfg.get("smtp_port", 587))
        self._user = cfg.get("smtp_user", "")
        self._password = cfg.get("smtp_password", "")
        self._from_addr = cfg.get("from_address", self._user)
        self._to_addrs = cfg.get("to_addresses", [])
        if isinstance(self._to_addrs, str):
            self._to_addrs = [self._to_addrs]
        self._enabled = cfg.get("enabled", False)
        self._min_level = cfg.get("min_level", "WARNING")  # INFO | WARNING | ERROR | CRITICAL

    def is_configured(self) -> bool:
        return bool(
            self._enabled and self._host and self._user and self._to_addrs
        )

    def _should_send(self, level: str) -> bool:
        order = ["INFO", "WARNING", "ERROR", "CRITICAL"]
        try:
            return order.index(level) >= order.index(self._min_level)
        except ValueError:
            return True

    def send(self, event: AlertEvent) -> bool:
        if not self.is_configured():
            return False
        if not self._should_send(event.level):
            return False
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[Apex Trader] {event.level}: {event.alert_type}"
            msg["From"] = self._from_addr
            msg["To"] = ", ".join(self._to_addrs)

            text = event.format_text()
            html = f"<pre>{text}</pre>"
            msg.attach(MIMEText(text, "plain"))
            msg.attach(MIMEText(html, "html"))

            with smtplib.SMTP(self._host, self._port, timeout=10) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(self._user, self._password)
                smtp.sendmail(self._from_addr, self._to_addrs, msg.as_string())

            logger.debug(f"[Email] Alert sent: {event.alert_type}")
            return True
        except Exception as e:
            logger.error(f"[Email] Failed to send alert: {e}")
            return False


# ---------------------------------------------------------------------------
# TELEGRAM CHANNEL
# ---------------------------------------------------------------------------

class TelegramAlertChannel(AlertChannel):
    """Telegram bot alert channel (HTTP API, no external deps)."""
    name = "telegram"

    def __init__(self, cfg: dict):
        self._token = cfg.get("bot_token", "")
        self._chat_id = str(cfg.get("chat_id", ""))
        self._enabled = cfg.get("enabled", False)
        self._min_level = cfg.get("min_level", "INFO")
        self._timeout = int(cfg.get("timeout_seconds", 10))

    def is_configured(self) -> bool:
        return bool(self._enabled and self._token and self._chat_id)

    def _should_send(self, level: str) -> bool:
        order = ["INFO", "WARNING", "ERROR", "CRITICAL"]
        try:
            return order.index(level) >= order.index(self._min_level)
        except ValueError:
            return True

    def send(self, event: AlertEvent) -> bool:
        if not self.is_configured():
            return False
        if not self._should_send(event.level):
            return False
        try:
            text = event.format_text()
            # Telegram supports up to 4096 chars
            if len(text) > 4000:
                text = text[:4000] + "\n...(truncated)"

            payload = json.dumps({
                "chat_id": self._chat_id,
                "text": text,
                "parse_mode": "HTML",
            }).encode("utf-8")

            url = f"https://api.telegram.org/bot{self._token}/sendMessage"
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                data = json.loads(resp.read())
                if data.get("ok"):
                    logger.debug(f"[Telegram] Alert sent: {event.alert_type}")
                    return True
                else:
                    logger.warning(f"[Telegram] API error: {data}")
                    return False
        except Exception as e:
            logger.error(f"[Telegram] Failed to send alert: {e}")
            return False


# ---------------------------------------------------------------------------
# DISCORD CHANNEL
# ---------------------------------------------------------------------------

class DiscordAlertChannel(AlertChannel):
    """Discord webhook alert channel."""
    name = "discord"

    def __init__(self, cfg: dict):
        self._webhook_url = cfg.get("webhook_url", "")
        self._enabled = cfg.get("enabled", False)
        self._min_level = cfg.get("min_level", "WARNING")
        self._timeout = int(cfg.get("timeout_seconds", 10))

        # Color by level
        self._colors = {
            "INFO":     3447003,   # Blue
            "WARNING":  16776960,  # Yellow
            "ERROR":    16711680,  # Red
            "CRITICAL": 10038562,  # Dark Red/Purple
        }

    def is_configured(self) -> bool:
        return bool(self._enabled and self._webhook_url)

    def _should_send(self, level: str) -> bool:
        order = ["INFO", "WARNING", "ERROR", "CRITICAL"]
        try:
            return order.index(level) >= order.index(self._min_level)
        except ValueError:
            return True

    def send(self, event: AlertEvent) -> bool:
        if not self.is_configured():
            return False
        if not self._should_send(event.level):
            return False
        try:
            color = self._colors.get(event.level, 0)
            description = event.message
            if event.details:
                detail_text = "\n".join(
                    f"**{k}**: {v}" for k, v in event.details.items()
                )
                description = f"{event.message}\n\n{detail_text}"

            if len(description) > 2000:
                description = description[:2000] + "\n...(truncated)"

            payload = json.dumps({
                "embeds": [{
                    "title": f"{event.level}: {event.alert_type}",
                    "description": description,
                    "color": color,
                    "footer": {"text": f"Apex Trader — {event.timestamp}"},
                }]
            }).encode("utf-8")

            req = urllib.request.Request(
                self._webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                status = resp.status
                if status in (200, 204):
                    logger.debug(f"[Discord] Alert sent: {event.alert_type}")
                    return True
                else:
                    logger.warning(f"[Discord] HTTP {status}")
                    return False
        except Exception as e:
            logger.error(f"[Discord] Failed to send alert: {e}")
            return False


# ---------------------------------------------------------------------------
# ALERT DISPATCHER
# ---------------------------------------------------------------------------

class AlertDispatcher:
    """
    Central alert dispatcher — routes events to all configured channels.

    Thread-safe. Async delivery (background thread) to avoid blocking loops.
    Persists alert history to disk for API visibility.
    """

    def __init__(self, config: dict):
        self._config = config
        self._lock = threading.Lock()
        self._channels: List[AlertChannel] = []
        self._history: List[dict] = []
        self._send_queue: List[AlertEvent] = []
        self._stop_event = threading.Event()

        alert_cfg = config.get("autonomy", {}).get("alerting", {})
        self._enabled = alert_cfg.get("enabled", True)
        self._max_history = alert_cfg.get("max_history", 500)

        # Load channels from config
        self._load_channels(alert_cfg)

        # History persistence
        data_dir = Path(config.get("data", {}).get("state_dir", "data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        self._history_file = data_dir / "alert_history.jsonl"

        # Background delivery thread
        self._delivery_thread = threading.Thread(
            target=self._delivery_loop,
            daemon=True,
            name="alert-delivery-loop",
        )
        self._delivery_thread.start()

        logger.info(
            f"[AlertDispatcher] Initialised — "
            f"channels={[c.name for c in self._channels if c.is_configured()]} "
            f"enabled={self._enabled}"
        )

    def _load_channels(self, alert_cfg: dict):
        """Build channel instances from config."""
        # Email
        email_cfg = alert_cfg.get("email", {})
        if email_cfg:
            self._channels.append(EmailAlertChannel(email_cfg))

        # Telegram
        tg_cfg = alert_cfg.get("telegram", {})
        if tg_cfg:
            self._channels.append(TelegramAlertChannel(tg_cfg))

        # Discord
        dc_cfg = alert_cfg.get("discord", {})
        if dc_cfg:
            self._channels.append(DiscordAlertChannel(dc_cfg))

    def emit(self, alert_type: str, message: str, details: dict):
        """
        Queue an alert for async delivery.
        Safe to call from any thread.
        """
        if not self._enabled:
            return
        level = ALERT_LEVELS.get(alert_type, "INFO")
        event = AlertEvent(
            alert_type=alert_type,
            message=message,
            details=details or {},
            level=level,
        )
        # Always log to console
        log_fn = {
            "CRITICAL": logger.critical,
            "ERROR": logger.error,
            "WARNING": logger.warning,
        }.get(level, logger.info)
        log_fn(f"[ALERT:{alert_type}] {message}")

        # Add to history
        with self._lock:
            self._history.append(event.to_dict())
            if len(self._history) > self._max_history:
                self._history.pop(0)
            self._send_queue.append(event)

        # Persist to file
        try:
            with self._history_file.open("a") as f:
                f.write(json.dumps(event.to_dict()) + "\n")
        except Exception:
            pass

    def _delivery_loop(self):
        """Background thread: drain send_queue and deliver to channels."""
        while not self._stop_event.is_set():
            try:
                with self._lock:
                    pending = list(self._send_queue)
                    self._send_queue.clear()

                for event in pending:
                    for channel in self._channels:
                        if channel.is_configured():
                            try:
                                ok = channel.send(event)
                                event.channel_results[channel.name] = ok
                            except Exception as e:
                                logger.debug(
                                    f"[AlertDispatcher] Channel {channel.name} error: {e}"
                                )
                                event.channel_results[channel.name] = False
            except Exception as e:
                logger.error(f"[AlertDispatcher] Delivery loop error: {e}")

            time.sleep(0.5)

    def stop(self):
        """Stop the delivery thread gracefully."""
        self._stop_event.set()
        self._delivery_thread.join(timeout=5)

    def get_history(self, limit: int = 50) -> List[dict]:
        """Return recent alert history (as plain dicts)."""
        with self._lock:
            return list(self._history[-limit:])

    def get_recent_alerts(self, limit: int = 50) -> List[AlertEvent]:
        """Return recent AlertEvent objects for API endpoints."""
        with self._lock:
            recent_dicts = list(self._history[-limit:])
        # Reconstruct AlertEvent objects from stored dicts
        result = []
        for d in recent_dicts:
            evt = AlertEvent(
                alert_type=d.get("alert_type", "UNKNOWN"),
                message=d.get("message", ""),
                details=d.get("details", {}),
                timestamp=d.get("timestamp", ""),
                level=d.get("level", "INFO"),
            )
            result.append(evt)
        return result

    def get_channel_status(self) -> List[dict]:
        """Return configuration status of all channels."""
        return [
            {
                "name": ch.name,
                "configured": ch.is_configured(),
                "type": type(ch).__name__,
            }
            for ch in self._channels
        ]


# ---------------------------------------------------------------------------
# MODULE-LEVEL SINGLETON
# ---------------------------------------------------------------------------

_dispatcher: Optional[AlertDispatcher] = None
_dispatcher_lock = threading.Lock()


def get_alert_dispatcher(config: Optional[dict] = None) -> AlertDispatcher:
    """Get or create the global AlertDispatcher singleton."""
    global _dispatcher
    if _dispatcher is None:
        with _dispatcher_lock:
            if _dispatcher is None:
                _dispatcher = AlertDispatcher(config or {})
    return _dispatcher

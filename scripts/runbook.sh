#!/usr/bin/env bash
# =============================================================================
# APEX MULTI-MARKET TJR ENGINE — Operations Runbook
# Phase 8: Always-On Process Management
#
# Usage: bash scripts/runbook.sh <command>
#
# Commands:
#   start           Start the trading engine via PM2
#   stop            Stop the engine gracefully
#   restart         Restart the engine
#   status          Show PM2 process status + health endpoint
#   logs            Tail recent logs (non-blocking, last 100 lines)
#   health          Curl the /health and /api/system/health endpoints
#   mode            Show current rollout mode
#   set-paper       Set rollout mode to UNATTENDED_PAPER_MODE
#   arm-check       Show live gate status (pre-checks for UNATTENDED_LIVE_MODE)
#   kill-switch     Engage kill switch (EMERGENCY STOP)
#   reset-paper     Reset to PAPER_MODE after kill switch
#   connect-oanda   Connect OANDA broker
#   preflight       Run OANDA pre-flight checks
#   scheduler       Show autonomous scheduler status
#   validate        Run full test suite
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
API_BASE="http://localhost:8080"
OPERATOR="${OPERATOR:-runbook}"

cd "$ROOT_DIR"

_curl() {
    curl -sf --max-time 10 "$@" 2>/dev/null || echo '{"error":"request failed or timed out"}'
}

_post() {
    local url="$1"
    local body="${2:-{}}"
    curl -sf --max-time 10 -X POST "$url" \
        -H "Content-Type: application/json" \
        -d "$body" 2>/dev/null || echo '{"error":"request failed"}'
}

cmd="${1:-help}"

case "$cmd" in

start)
    echo "▶ Starting Apex Trader via PM2..."
    pm2 start ecosystem.config.cjs
    sleep 3
    pm2 status
    echo ""
    echo "Health check:"
    _curl "$API_BASE/health" | python3 -m json.tool 2>/dev/null || true
    ;;

stop)
    echo "■ Stopping Apex Trader..."
    pm2 stop apex-trader || true
    pm2 status
    ;;

restart)
    echo "↻ Restarting Apex Trader..."
    pm2 restart apex-trader
    sleep 3
    pm2 status
    ;;

status)
    echo "=== PM2 Status ==="
    pm2 status
    echo ""
    echo "=== API Health ==="
    _curl "$API_BASE/health" | python3 -m json.tool 2>/dev/null || echo "(API not responding)"
    ;;

logs)
    echo "=== Recent PM2 Logs (last 100 lines) ==="
    pm2 logs apex-trader --lines 100 --nostream
    ;;

health)
    echo "=== /health ==="
    _curl "$API_BASE/health" | python3 -m json.tool
    echo ""
    echo "=== /api/system/health ==="
    _curl "$API_BASE/api/system/health" | python3 -m json.tool
    ;;

mode)
    echo "=== Rollout Mode ==="
    _curl "$API_BASE/api/system/mode" | python3 -m json.tool
    ;;

set-paper)
    echo "→ Setting UNATTENDED_PAPER_MODE..."
    _post "$API_BASE/api/system/mode" \
        "{\"mode\":\"UNATTENDED_PAPER_MODE\",\"operator\":\"$OPERATOR\",\"reason\":\"runbook\"}" \
        | python3 -m json.tool
    ;;

arm-check)
    echo "=== Live Gate Pre-Check ==="
    _curl "$API_BASE/api/system/mode/live-gate" | python3 -m json.tool
    echo ""
    echo "=== Execution State ==="
    _curl "$API_BASE/api/execution/state" | python3 -m json.tool
    ;;

kill-switch)
    echo "🔴 ENGAGING KILL SWITCH..."
    read -p "Confirm kill switch [yes/no]: " confirm
    if [[ "$confirm" != "yes" ]]; then
        echo "Aborted."
        exit 0
    fi
    _post "$API_BASE/api/control/kill" \
        "{\"reason\":\"Manual kill switch via runbook\",\"operator\":\"$OPERATOR\"}" \
        | python3 -m json.tool
    echo ""
    echo "Kill switch engaged. All new orders blocked."
    ;;

reset-paper)
    echo "↩ Resetting to PAPER_MODE..."
    _post "$API_BASE/api/control/resume" \
        "{\"reason\":\"Manual reset to paper via runbook\",\"operator\":\"$OPERATOR\"}" \
        | python3 -m json.tool
    ;;

connect-oanda)
    echo "→ Connecting OANDA broker..."
    _post "$API_BASE/api/broker/connect" \
        "{\"broker\":\"oanda\",\"operator\":\"$OPERATOR\"}" \
        | python3 -m json.tool
    ;;

preflight)
    echo "→ Running OANDA preflight checks..."
    _post "$API_BASE/api/broker/preflight" '{}' | python3 -m json.tool
    ;;

scheduler)
    echo "=== Autonomous Scheduler Status ==="
    _curl "$API_BASE/api/system/scheduler" | python3 -m json.tool
    ;;

validate)
    echo "=== Running Full Test Suite ==="
    cd "$ROOT_DIR"
    python3 -m pytest tests/ -v --tb=short 2>&1 | tail -30
    ;;

help|*)
    echo "Apex Trader Operations Runbook"
    echo ""
    echo "Usage: bash scripts/runbook.sh <command>"
    echo ""
    echo "Commands:"
    echo "  start        Start engine via PM2"
    echo "  stop         Stop engine"
    echo "  restart      Restart engine"
    echo "  status       PM2 status + API health"
    echo "  logs         Recent logs (non-blocking)"
    echo "  health       Full health check"
    echo "  mode         Show rollout mode"
    echo "  set-paper    Set UNATTENDED_PAPER_MODE"
    echo "  arm-check    Pre-check for live mode"
    echo "  kill-switch  EMERGENCY STOP"
    echo "  reset-paper  Reset after kill switch"
    echo "  connect-oanda Connect OANDA"
    echo "  preflight    Run OANDA preflight"
    echo "  scheduler    Scheduler status"
    echo "  validate     Run test suite"
    ;;

esac

#!/bin/bash
# =============================================================================
# APEX MULTI-MARKET TJR ENGINE — First Run Wizard
# =============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║       APEX MULTI-MARKET TJR ENGINE — First Run Wizard        ║${NC}"
echo -e "${BLUE}║              Autonomous Trading System v1.0.0                ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

cd "$PROJECT_DIR"

# Step 1: Check Python
echo -e "${YELLOW}[1/7] Checking Python environment...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: Python 3 not found. Install Python 3.10+${NC}"
    exit 1
fi
PYTHON_VER=$(python3 --version)
echo -e "${GREEN}✓ $PYTHON_VER${NC}"

# Step 2: Install dependencies
echo ""
echo -e "${YELLOW}[2/7] Installing Python dependencies...${NC}"
pip3 install -q fastapi uvicorn pyyaml pydantic structlog apscheduler sqlalchemy numpy pandas 2>&1 | tail -5
echo -e "${GREEN}✓ Dependencies installed${NC}"

# Step 3: Create required directories
echo ""
echo -e "${YELLOW}[3/7] Creating directory structure...${NC}"
mkdir -p logs reports data/historical
echo -e "${GREEN}✓ Directories ready${NC}"

# Step 4: Verify configuration
echo ""
echo -e "${YELLOW}[4/7] Verifying system configuration...${NC}"
if [ ! -f "config/system_config.yaml" ]; then
    echo -e "${RED}ERROR: config/system_config.yaml not found${NC}"
    exit 1
fi

# Check environment setting
ENVIRONMENT=$(python3 -c "import yaml; c=yaml.safe_load(open('config/system_config.yaml')); print(c.get('system',{}).get('environment','paper'))")
echo -e "${GREEN}✓ Environment: $ENVIRONMENT${NC}"

if [ "$ENVIRONMENT" = "live" ]; then
    echo -e "${RED}⚠️  WARNING: System configured for LIVE trading${NC}"
    echo -e "${YELLOW}   Ensure paper trading validation is complete before live trading${NC}"
fi

# Step 5: Run unit tests
echo ""
echo -e "${YELLOW}[5/7] Running unit tests...${NC}"
if python3 -m pytest tests/unit/ -q --tb=short 2>&1; then
    echo -e "${GREEN}✓ All unit tests passed${NC}"
else
    echo -e "${YELLOW}⚠️  Some tests failed — review before production use${NC}"
fi

# Step 6: Quick smoke test (backtest on synthetic data)
echo ""
echo -e "${YELLOW}[6/7] Running smoke test backtest...${NC}"
python3 -c "
import sys
sys.path.insert(0, '.')
from backtest.backtest_engine import BacktestEngine
from domain.models import InstrumentType, VenueType
import yaml

with open('config/system_config.yaml') as f:
    config = yaml.safe_load(f)

engine = BacktestEngine(config)
result = engine.run(
    instrument='XAUUSD',
    instrument_type=InstrumentType.GOLD,
    venue_type=VenueType.CFD_BROKER,
    timeframe='M15'
)
m = result['metrics']
print(f'  Trades: {m[\"total_trades\"]}')
print(f'  Win Rate: {m[\"win_rate\"]:.1%}')
print(f'  Profit Factor: {m[\"profit_factor\"]:.2f}')
print(f'  Net P&L: \${m[\"net_profit_usd\"]:+.2f}')
print(f'  Max DD: {m[\"max_drawdown_pct\"]:.2f}%')
"
echo -e "${GREEN}✓ Smoke test complete${NC}"

# Step 7: Start the server
echo ""
echo -e "${YELLOW}[7/7] Starting Apex Trader API server...${NC}"
echo ""
echo -e "${CYAN}  Dashboard:  http://localhost:8080/dashboard${NC}"
echo -e "${CYAN}  API:        http://localhost:8080/api/status${NC}"
echo -e "${CYAN}  Webhook:    POST http://localhost:8080/api/signal${NC}"
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Apex Multi-Market TJR Engine is starting...${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

python3 live/api_server.py

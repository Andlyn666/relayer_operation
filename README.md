# Relayer Operation Tools

A collection of tools for monitoring and managing cryptocurrency relayer operations.

## Features

- Daily profit calculation
- APY tracking
- Balance monitoring
- Multiple token support (ETH, WBTC, USDC, DAI, USDT)
- Chain support (Ethereum, Base, Arbitrum, Optimism)
- Automated alerts
- Excel report generation

## Installation

1. Clone the repository
```bash
git clone [repository-url]
cd relayer_operation
```

2. Set up Python virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

## Usage

### Daily Calculations
```bash
./run_daily.sh
```

### Balance Monitoring
```bash
./check_balance.sh
```

### Manual Report Generation
```bash
python calc_daily.py
python calc_apy.py
python calc_return.py
```

## Configuration

- Adjust token addresses in 

calc_daily.py


- Set alert thresholds in 

send_alert.py


- Configure cron jobs for automated execution

## Directory Structure

```
relayer_operation/
├── calc_daily.py      # Daily profit calculations
├── calc_apy.py        # APY calculations
├── calc_return.py     # Return calculations
├── send_alert.py      # Alert system
├── check_balance.sh   # Balance check script
├── run_daily.sh       # Daily execution script
└── requirements.txt   # Python dependencies
```

## Requirements

- Python 3.9+
- SQLite3
- Excel support libraries
- Web3 support
- Pandas

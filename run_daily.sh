#!/bin/bash
cd ~/relayer_operation
source venv/bin/activate
python calc_daily.py
deactivate
@echo off
cd /d "%~dp0"
python -m streamlit run tw_stock_v31.py --server.address 0.0.0.0 --server.port 8502
pause
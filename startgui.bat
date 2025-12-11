@echo off
REM Start master in background (new window)
start cmd /k "python master.py"
REM Start GUI
python gui.py

@echo off
echo Starting distributed web crawler worker...
echo Connecting to server at 127.0.0.1:5555

python remote_worker.py

echo Worker process terminated.
pause 
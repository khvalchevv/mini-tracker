@echo off
rem Start the Bitvavo bot from its own folder; stderr (tracebacks) goes to logs\bot.err,
rem everything else is in logs\bot.log. Closing this window stops the bot.
cd /d "%~dp0"
"C:\Users\User\AppData\Local\Programs\Python\Python313\python.exe" main.py 2>> logs\bot.err

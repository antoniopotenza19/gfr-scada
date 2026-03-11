@echo off
powershell -ExecutionPolicy Bypass -File "%~dp0scripts\start-local-vpn.ps1" %*

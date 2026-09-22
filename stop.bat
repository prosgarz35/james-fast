@echo off
rem ==============================================================================
rem  Apache James Server - Graceful Shutdown (Portable Mode)
rem ==============================================================================
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop.ps1"

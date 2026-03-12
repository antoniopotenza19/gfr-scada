@echo off
setlocal

set "ROOT=%~dp0"
set "PYTHON_EXE=%ROOT%venv\Scripts\python.exe"

if not exist "%PYTHON_EXE%" (
  echo Python virtualenv non trovato.
  echo Atteso: venv\Scripts\python.exe nella root del repository
  exit /b 1
)

pushd "%ROOT%"
"%PYTHON_EXE%" "%ROOT%backend\app\scripts\csv_to_db_ingestor.py"
set "EXIT_CODE=%ERRORLEVEL%"
popd

if not "%EXIT_CODE%"=="0" (
  echo.
  echo Lo script e' terminato con errore: %EXIT_CODE%
)

exit /b %EXIT_CODE%

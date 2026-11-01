@echo off
rem ==============================================================================
rem run.bat — Unified execution and lifecycle management for Workshop 3 (Windows)
rem Supports Docker Compose and Podman Compose
rem ==============================================================================
setlocal enabledelayedexpansion

cd /d "%~dp0"

if not exist "logs" mkdir "logs"
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set dt=%%I
set "TIMESTAMP=%dt:~0,4%%dt:~4,2%%dt:~6,2%_%dt:~8,2%%dt:~10,2%%dt:~12,2%"
set "LOG_FILE=logs\run_%TIMESTAMP%.log"

echo [INFO] Log session: %LOG_FILE%
echo [INFO] Workshop 3 Windows Execution Script >> "%LOG_FILE%"

rem Detect container tool
set "COMPOSE_CMD=docker compose"
docker compose version >nul 2>&1
if errorlevel 1 (
    podman-compose version >nul 2>&1
    if not errorlevel 1 (
        set "COMPOSE_CMD=podman-compose"
    ) else (
        docker-compose version >nul 2>&1
        if not errorlevel 1 (
            set "COMPOSE_CMD=docker-compose"
        )
    )
)
echo [INFO] Detected compose tool: %COMPOSE_CMD%
echo [INFO] Detected compose tool: %COMPOSE_CMD% >> "%LOG_FILE%"

rem Python interpreter
set "PY_CMD=python"
if exist ".venv\Scripts\python.exe" set "PY_CMD=.venv\Scripts\python.exe"

set "ARG=%~1"
if "%ARG%"=="" goto MENU
if /i "%ARG%"=="auto" goto RUN_AUTO
if /i "%ARG%"=="step" goto RUN_STEP
if /i "%ARG%"=="start" goto INFRA_START
if /i "%ARG%"=="start-all" goto INFRA_START_ALL
if /i "%ARG%"=="stop" goto INFRA_STOP
if /i "%ARG%"=="clean" goto INFRA_CLEAN
if /i "%ARG%"=="recreate" goto INFRA_RECREATE
if /i "%ARG%"=="test" goto RUN_TEST
if /i "%ARG%"=="airflow-success" goto AIRFLOW_SUCCESS
if /i "%ARG%"=="airflow-fail" goto AIRFLOW_FAIL
if /i "%ARG%"=="status" goto INFRA_STATUS
if /i "%ARG%"=="help" goto USAGE
goto USAGE

:MENU
cls
echo ==================================================================
echo       Workshop 3 - Streaming ETL (Kafka + ML + PostgreSQL)
echo ==================================================================
echo   1) Complete Automatic Execution (Pipeline End-to-End)
echo   2) Step-by-Step Interactive Execution
echo   3) Start Base Infrastructure (Kafka + PostgreSQL)
echo   4) Start All Services (Airflow, Superset, Monitoring)
echo   5) Stop Containers
echo   6) Clean Containers and Persistent Volumes (Reset)
echo   7) Recreate Project from Scratch
echo   8) Run Contract Tests (pytest)
echo   9) Airflow DAG: Success Demo (happiness_batch_training)
echo  10) Airflow DAG: Controlled Failure Demo (happiness_batch_failure_demo)
echo  11) Show System Status (Containers + Database)
echo   0) Exit
echo ==================================================================
set /p "CHOICE=Select an option [0-11]: "
if "%CHOICE%"=="1" goto RUN_AUTO
if "%CHOICE%"=="2" goto RUN_STEP
if "%CHOICE%"=="3" goto INFRA_START
if "%CHOICE%"=="4" goto INFRA_START_ALL
if "%CHOICE%"=="5" goto INFRA_STOP
if "%CHOICE%"=="6" goto INFRA_CLEAN
if "%CHOICE%"=="7" goto INFRA_RECREATE
if "%CHOICE%"=="8" goto RUN_TEST
if "%CHOICE%"=="9" goto AIRFLOW_SUCCESS
if "%CHOICE%"=="10" goto AIRFLOW_FAIL
if "%CHOICE%"=="11" goto INFRA_STATUS
if "%CHOICE%"=="0" exit /b 0
echo Invalid option.
pause
goto MENU

:USAGE
echo Usage: run.bat [auto^|step^|start^|start-all^|stop^|clean^|recreate^|test^|airflow-success^|airflow-fail^|status]
exit /b 1

:INFRA_START
echo [STEP] Starting base infrastructure (Kafka + Postgres)...
echo [STEP] Starting base infrastructure >> "%LOG_FILE%"
%COMPOSE_CMD% up -d kafka kafka-init postgres >> "%LOG_FILE%" 2>&1
echo [OK] Base infrastructure started.
goto END

:INFRA_START_ALL
echo [STEP] Starting all services (monitoring, dashboards, airflow)...
%COMPOSE_CMD% --profile monitoring --profile dashboards --profile airflow up -d --build >> "%LOG_FILE%" 2>&1
echo [OK] All services started.
goto END

:INFRA_STOP
echo [STEP] Stopping containers...
%COMPOSE_CMD% --profile monitoring --profile dashboards --profile airflow stop >> "%LOG_FILE%" 2>&1
echo [OK] Containers stopped.
goto END

:INFRA_CLEAN
echo [WARN] Removing containers and persistent volumes...
%COMPOSE_CMD% --profile monitoring --profile dashboards --profile airflow down -v --remove-orphans >> "%LOG_FILE%" 2>&1
echo [OK] Clean finished.
goto END

:INFRA_RECREATE
echo [STEP] Recreating project from scratch...
call :INFRA_CLEAN
call :INFRA_START
echo [STEP] Running initial batch pipeline...
%PY_CMD% -m src.harmonize >> "%LOG_FILE%" 2>&1
%PY_CMD% -m src.train >> "%LOG_FILE%" 2>&1
echo [OK] Recreate finished.
goto END

:RUN_TEST
echo [STEP] Running contract tests...
%PY_CMD% -m pytest -q >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [ERROR] Tests failed. See %LOG_FILE%
) else (
    echo [OK] All tests passed.
)
goto END

:AIRFLOW_SUCCESS
echo [STEP] Running Airflow DAG Success Demo: happiness_batch_training...
%COMPOSE_CMD% --profile airflow exec airflow-scheduler airflow dags test happiness_batch_training >> "%LOG_FILE%" 2>&1
echo [OK] Airflow DAG execution finished.
goto END

:AIRFLOW_FAIL
echo [STEP] Running Airflow DAG Controlled Failure Demo: happiness_batch_failure_demo...
echo [INFO] Great Expectations will detect injected defect and safely halt pipeline.
%COMPOSE_CMD% --profile airflow exec airflow-scheduler airflow dags test happiness_batch_failure_demo >> "%LOG_FILE%" 2>&1
echo [OK] Failure demo finished. Check %LOG_FILE% to see error isolation.
goto END

:INFRA_STATUS
echo [STEP] Container Status:
%COMPOSE_CMD% ps
goto END

:RUN_AUTO
echo ==================================================================
echo [STEP] Running Complete Automatic Pipeline
echo ==================================================================
call :INFRA_START
echo [STEP] 1. Harmonizing 2015-2019 CSVs...
%PY_CMD% -m src.harmonize >> "%LOG_FILE%" 2>&1
echo [STEP] 2. Validating with Great Expectations...
%PY_CMD% -m src.validate >> "%LOG_FILE%" 2>&1
echo [STEP] 3. Training model and exporting test stream...
%PY_CMD% -m src.train >> "%LOG_FILE%" 2>&1
echo [STEP] 4. Running pytest suite...
%PY_CMD% -m pytest -q >> "%LOG_FILE%" 2>&1
echo [STEP] 5. Starting streaming demo (Producer with invalid events)...
start /B "consumer" %PY_CMD% kafka\consumer.py >> "%LOG_FILE%" 2>&1
timeout /t 3 >nul
%PY_CMD% kafka\producer.py --delay 0.05 --run-id win_demo --inject-invalid >> "%LOG_FILE%" 2>&1
timeout /t 4 >nul
taskkill /F /IM python.exe /FI "WINDOWTITLE eq consumer" >nul 2>&1
echo [STEP] 6. Regenerating KPI charts...
%PY_CMD% dashboards\kpi_charts.py >> "%LOG_FILE%" 2>&1
echo [OK] Complete Automatic Pipeline execution finished!
echo [OK] Detailed log at: %LOG_FILE%
goto END

:RUN_STEP
echo ==================================================================
echo [STEP] Interactive Step-by-Step Execution
echo ==================================================================
set /p "ANS=Execute Step 1: Start Infrastructure (Kafka + Postgres)? [Y/N]: "
if /i "%ANS%"=="Y" call :INFRA_START

set /p "ANS=Execute Step 2: Harmonize CSVs? [Y/N]: "
if /i "%ANS%"=="Y" %PY_CMD% -m src.harmonize

set /p "ANS=Execute Step 3: Great Expectations validation? [Y/N]: "
if /i "%ANS%"=="Y" %PY_CMD% -m src.validate

set /p "ANS=Execute Step 4: Train ML Model? [Y/N]: "
if /i "%ANS%"=="Y" %PY_CMD% -m src.train

set /p "ANS=Execute Step 5: Run Pytest tests? [Y/N]: "
if /i "%ANS%"=="Y" %PY_CMD% -m pytest -q

set /p "ANS=Execute Step 6: Regenerate KPI charts? [Y/N]: "
if /i "%ANS%"=="Y" %PY_CMD% dashboards\kpi_charts.py

echo [OK] Step-by-step execution finished.
goto END

:END
if "%ARG%"=="" pause
exit /b 0

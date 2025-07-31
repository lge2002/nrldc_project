@echo off
REM Step 1: Set Java environment
set JAVA_HOME=C:\Program Files\Microsoft\jdk-11.0.28.6-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%

REM Step 2: Activate virtual environment (adjust path if needed)
call C:\Users\tamilarasans\Desktop\NRLDC\venv\Scripts\activate.bat

REM Step 3: Navigate to Django project folder
cd /d C:\Users\tamilarasans\Desktop\NRLDC

REM Step 4: Run the management command
python manage.py nrldc_project

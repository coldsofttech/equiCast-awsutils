@echo off
setlocal

REM Define the files to be deleted
set "FLAKE_FILE=flake8-report"
set "PYTEST_FILE=pytest_results.html"
set "BUILD=dist"
set "DIST_PACKAGE=dist\*.tar.gz"

REM Check if an argument is provided
if "%~1"=="" (
    echo Usage: %0 [True|False]
    exit /b 1
)

REM Parse the optional argument
set "OUTPUT=%~1"

REM Validate the argument
if /I "%OUTPUT%" neq "True" if /I "%OUTPUT%" neq "False" (
    echo Invalid argument. Please specify True or False.
    exit /b 1
)

REM Define the files and directories to be checked
echo Verifying if all mandatory files exist...
set "CHECK_FILES_DIRS=tests .flake8 .gitignore LICENSE pytest.ini README.md requirements.txt setup.py"

REM Check for the existence of each required file or directory
for %%f in (%CHECK_FILES_DIRS%) do (
    if not exist "%%f" (
        echo Error: Required file or directory %%f is missing.
        exit /b 1
    )
)

REM Execute flake8 command based on the argument
if /I "%OUTPUT%"=="True" (
    REM Delete the files if they exist
    if exist "%FLAKE_FILE%" (
        echo Deleting %FLAKE_FILE%...
        del "%FLAKE_FILE%"
    )

    if exist "%PYTEST_FILE%" (
        echo Deleting %PYTEST_FILE%...
        del "%PYTEST_FILE%"
    )

    REM Execute flake8 with output redirection to a file
    echo Running flake8 with output file...
    flake8 --verbose --color auto --count --statistics --format=default --output-file=flake8-report
) else (
    REM Execute flake8 without output redirection
    echo Running flake8 without output file...
    flake8 --verbose --color auto --count --statistics --format=default
)

REM Build package
if exist %BUILD% (
    echo Deleting folder %BUILD% and its contents...
    rd /s /q "%BUILD%"
)
echo Building package...
python setup.py sdist

REM Install the generated distribution
echo Installing the package...
for %%f in (%DIST_PACKAGE%) do (
    echo Installing %%f...
    pip install -e .
)

REM Execute pytest with output redirection to a file
echo Running pytest with output file...
pytest tests -vv -rEPW -o pytest_collection_order=alphabetical --cache-clear --color=yes --html=pytest_results.html --self-contained-html

endlocal
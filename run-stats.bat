@echo off
echo ============================================
echo  MSU Baseball Stats Scraper (incremental)
echo  Only scrapes new games not already cached.
echo  For a full re-scrape, use run-stats-full.bat
echo ============================================
echo.
cd /d "%~dp0"

.venv\Scripts\python.exe scripts\scrape-stats.py
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Scraper failed. Skipping git commit.
    pause
    exit /b 1
)

echo.
echo Committing and pushing updated stats to GitHub...
git add public\data\*.json
git diff --cached --quiet
if %errorlevel% equ 0 (
    echo No changes to commit - stats already up to date.
) else (
    for /f "tokens=1-3 delims=/ " %%a in ("%date%") do set TODAY=%%c-%%a-%%b
    git commit -m "Stats update %TODAY%"
    git push
    echo.
    echo Pushed! Cloudflare will deploy the updated stats in about a minute.
)

echo.
echo ============================================
echo  Done! JSON files saved to public\data\
echo  Cache saved to data\scrape-cache.json
echo ============================================
pause

@echo off
echo ============================================
echo  MSU Baseball Stats Scraper (FULL re-scrape)
echo  Ignores cache and re-scrapes ALL games.
echo  Use this if stats were corrected on NCAA site
echo  or if the cache seems wrong.
echo  For a normal incremental run, use run-stats.bat
echo ============================================
echo.
cd /d "%~dp0"

.venv\Scripts\python.exe scripts\scrape-stats.py --full
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
    git commit -m "Stats update %TODAY% (full re-scrape)"
    git push
    echo.
    echo Pushed! Cloudflare will deploy the updated stats in about a minute.
)

echo.
echo ============================================
echo  Done! JSON files saved to public\data\
echo  Cache rebuilt at data\scrape-cache.json
echo ============================================
pause

$ErrorActionPreference = "Stop"
Set-Location "D:\VSC Programs\FratFinderAI\services\crawler"
python -m fratfinder_crawler.cli run-field-job-worker --limit 30 --workers 2 --poll-seconds 10 --runtime-mode langgraph_primary --graph-durability sync

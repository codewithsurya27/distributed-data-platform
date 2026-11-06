$ErrorActionPreference = "Stop"

Write-Host "`n[1/3] Building Spring Boot application..." -ForegroundColor Cyan

.\gradlew.bat clean bootJar -x test

if ($LASTEXITCODE -ne 0) {
    throw "Gradle build failed. Docker deployment cancelled."
}

Write-Host "`n[2/3] Building and starting Docker services..." -ForegroundColor Cyan

docker compose -f compose.yml up -d --build

if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose failed."
}

Write-Host "`n[3/3] Checking container status..." -ForegroundColor Cyan

docker compose -f compose.yml ps

Write-Host "`nApplication deployment completed." -ForegroundColor Green
Write-Host "Application URL: http://localhost:8081"

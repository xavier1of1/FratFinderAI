param(
  [string]$ContainerName = "fratfinder-postgres",
  [string]$DbUser = "postgres",
  [string]$DbName = "fratfinder"
)

$migrations = Get-ChildItem "infra/supabase/migrations" -Filter "*.sql" | Sort-Object Name
foreach ($migration in $migrations) {
  Write-Host "Applying migration: $($migration.Name)"
  Get-Content -Raw $migration.FullName | docker exec -i $ContainerName psql -v ON_ERROR_STOP=1 -U $DbUser -d $DbName
}
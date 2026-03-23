param(
  [string]$ContainerName = "fratfinder-postgres",
  [string]$DbUser = "postgres",
  [string]$DbName = "fratfinder"
)

$seeds = Get-ChildItem "infra/supabase/seeds" -Filter "*.sql" | Sort-Object Name
foreach ($seed in $seeds) {
  Write-Host "Applying seed: $($seed.Name)"
  Get-Content -Raw $seed.FullName | docker exec -i $ContainerName psql -v ON_ERROR_STOP=1 -U $DbUser -d $DbName
}
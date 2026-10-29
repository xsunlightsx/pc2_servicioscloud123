# Registra cada N segundos la evidencia de la prueba 1 -> 2 -> 1 en evidencias\elasticidad.csv
# Uso (desde C:\pc2-ventas):
#   powershell -ExecutionPolicy Bypass -File scripts\registrar_elasticidad.ps1
# Detener con Ctrl+C. Requiere la web abierta en http://localhost:30080
param([int]$Intervalo = 5, [string]$Salida = "evidencias\elasticidad.csv")

$ns = "ventas"
$carpeta = Split-Path $Salida
if ($carpeta -and -not (Test-Path $carpeta)) { New-Item -ItemType Directory -Path $carpeta | Out-Null }
if (-not (Test-Path $Salida)) {
  "hora,fase,tasa_envio,enviadas,unicas,mensajes_listos,sin_confirmar,consumidores,replicas_deseadas_hpa,replicas_deployment,pods_listos" | Out-File -FilePath $Salida -Encoding utf8
}
Write-Host "Registrando cada $Intervalo s en $Salida (Ctrl+C para detener)"

while ($true) {
  try {
    $e   = Invoke-RestMethod -Uri "http://localhost:30080/api/estado" -TimeoutSec 5
    $hpa = kubectl get hpa keda-hpa-procesador-scaler -n $ns -o jsonpath="{.status.desiredReplicas}" 2>$null
    $dep = kubectl get deployment procesador -n $ns -o jsonpath="{.spec.replicas}" 2>$null
    $lis = kubectl get deployment procesador -n $ns -o jsonpath="{.status.readyReplicas}" 2>$null
    if (-not $lis) { $lis = 0 }
    $linea = "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10}" -f (Get-Date -Format "HH:mm:ss"), `
      $e.generador.fase, $e.generador.tasa_actual, $e.generador.confirmadas, $e.db.unicas, `
      $e.rabbit.listos, $e.rabbit.sin_confirmar, $e.rabbit.consumidores, $hpa, $dep, $lis
    Add-Content -Path $Salida -Value $linea -Encoding utf8
    Write-Host $linea
  } catch { Write-Host "Error: $_" }
  Start-Sleep -Seconds $Intervalo
}

# PC2 - Procesamiento de ventas con página web y elasticidad automática

Curso: Servicios Cloud (100000SI97) - Docente: Ing. Jorge Fuentes Pinto - Periodo: 2026 ciclo 2

Solución local en Docker Desktop con Kubernetes (un solo nodo). Un generador Python envía 10 000 ventas a RabbitMQ; procesadores de ventas en Python las validan y guardan en PostgreSQL; KEDA y HPA escalan los procesadores entre 1 y 2 réplicas según la cola; una página Flask controla y monitorea la simulación.

---

## 1. Integrantes

| Integrante | Código | Aporte |
|---|---|---|
| Félix Francisco Córdova Pachas | U23234449 | KEDA y verificación |
| Solange Ariana Cepeda Laynes | U23211714 | Configuración de Kubernetes y Docker |
| Jhanayra Jariv Gomero Donayre | U23211774 | Repositorio |
| Esthepfany Angelina Macollunco Prado | U23252590 | Python y HPA |

**Repositorio:** https://github.com/xsunlightsx/pc2_servicioscloud123.git

---

## 2. Requisitos

**Hardware (recomendado):** CPU de 4 núcleos o más, 8 GB de RAM, SSD con 50 GB libres, virtualización habilitada. Mantener la laptop enchufada durante la corrida larga.

**Software:**

| Herramienta | Versión usada |
|---|---|
| Windows + PowerShell | `<11 + 7>` |
| Docker Desktop (con Kubernetes activado, un nodo `docker-desktop`) | `<29.9.2>` |
| kubectl | `<v1.37.1>` |
| Helm (para instalar KEDA) | `<v4.3.1>` |
| KEDA | 2.21.0 |
| RabbitMQ | imagen `rabbitmq:3.13-management` |
| PostgreSQL | imagen `<postgres:16>` |
| Python (generador, procesador, web) | `<3.15.2>` |
| Flask | `<3.1.3>` |

---

## 3. Arquitectura

Todo vive en el namespace **`ventas`**.

| Componente | Recurso | Puerto | Responsabilidad |
|---|---|---|---|
| Generador Python | Deployment `generador` | - | Crea y envía ventas a la cola |
| RabbitMQ | Deployment + Service `rabbitmq` (ClusterIP) | 5672 (AMQP), 15672 (gestión) | Conserva y distribuye tareas |
| Procesadores | Deployment `procesador` (1 a 2 réplicas) | - | Validan y procesan ventas, ack manual, prefetch 1 |
| PostgreSQL | Deployment + Service `postgres` + PVC | 5432 | Guarda resultados; `id_venta` es PRIMARY KEY |
| Página web Flask | Deployment `web` + Service NodePort | 30080 (acceso desde la laptop) | Inicio, Pausa, Continuar, ritmo, avance y totales |
| KEDA + HPA | ScaledObject `procesador-scaler`, HPA `keda-hpa-procesador-scaler` | - | Escalan `procesador` entre 1 y 2 según `QueueLength` |

**Comunicación:** los servicios internos se hablan por nombre (`rabbitmq`, `postgres`). Solo la web se expone a la laptop (`http://localhost:30080`), porque es la única interfaz que el usuario necesita. RabbitMQ y PostgreSQL permanecen internos.

**Diagrama:** ver `docs/diagrama-arquitectura.png` (componentes, conexiones, puertos y volúmenes).

**Esquema de la tabla `ventas`:** `id_venta` (PK), `fecha_hora`, `sucursal`, `categoria`, `cantidad` (> 0), `precio_unitario_centimos`, `importe_centimos`, `procesador`, `procesado_en`. Los importes se manejan en céntimos enteros.

**Diseño de confiabilidad:**
- El procesador guarda la venta en PostgreSQL y **solo después** confirma el mensaje (ack manual, prefetch 1).
- La inserción usa la PRIMARY KEY (`ON CONFLICT DO NOTHING`), así que una reentrega o un reenvío no duplica el resultado.
- Cierre ordenado ante SIGTERM.

**Parámetros de recursos y elasticidad:**
- RabbitMQ: requests `200m` CPU / `256Mi`; limits `1` CPU / `512Mi`.
- Readiness probe de RabbitMQ: `initialDelaySeconds: 30`, `periodSeconds: 15`, `timeoutSeconds: 10`, `failureThreshold: 6` (la versión inicial con timeout de 1 s fallaba bajo carga).
- ScaledObject: `minReplicaCount: 1`, `maxReplicaCount: 2`, trigger RabbitMQ `QueueLength`, objetivo **100 mensajes por réplica**.
- HPA scale down: estabilización de **120 s**, 1 pod cada 30 s. Scale up: ventana de 0 s.
- Simulación: 0,2 s de validación externa por venta, un mensaje a la vez por pod (capacidad aproximada de 5 ventas/s por pod).

---

## 4. Estructura de la carpeta

```
pc2_servicioscloud123/
├── README.md
├── k8s/                      Manifiestos (namespace, secretos, RabbitMQ, PostgreSQL + PVC, generador, procesador, web, ScaledObject)
│   └──00-namespace.yaml
│   └──10-postgres.yaml
│   └──20-rabbitmq.yaml
│   └──30-procesador.yaml
│   └──40-generador.yaml
│   └──50-web.yaml
│   └──60-keda.yaml
│   └──pod-pending.yaml
├── procesador/                      Código fuente (generador, procesador, web Flask) y Dockerfiles
│   └──Dockerfile
│   └──procesador.py
│   └──requirements.txt
├── generador/
│   └──Dockerfile
│   └──generador.py
│   └──requirements.txt
├── data/
│   ├── ventas.csv            10 000 ventas válidas (mismo archivo para todas las pruebas)
│   └── referencia.json          Cálculo de referencia independiente (totales esperados)
├── scripts/
│   └── registrar_elasticidad.ps1   Registro en CSV cada 5 s durante la prueba 1 -> 2 -> 1
│   └──generar_csv.py
│   └──publicar_prueba.py
│   └──referencia.py
├── evidencias/               Capturas, logs, CSV de elasticidad y consultas SQL
│   └──elasticidad.csv
│   └──totales_corrida_10000.txt
│   └──procesadores_corrida_10000.txt
└── web/                     Diagrama de arquitectura
│   └──static/
│   │  └── index.html
│   └──Dockerfile
│   └──requirements.txt
│   └──web.py
└──.gitattributes
```

---

## 5. Despliegue reproducible

Ejecutar desde la raíz del proyecto en PowerShell.

**0. Clonar el repositorio**
```powershell
git clone https://github.com/xsunlightsx/pc2_servicioscloud123.git
cd pc2_servicioscloud123
```

**1. Verificar Kubernetes**
```powershell
kubectl config use-context docker-desktop
kubectl get nodes
```

**2. Construir las imágenes locales** (ajustar nombres a los de los Dockerfiles)
```powershell
docker build -t ventas-generador:1.0 app/generador
docker build -t ventas-procesador:1.0 app/procesador
docker build -t ventas-web:1.0 app/web
```

**3. Instalar KEDA**
```powershell
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
kubectl get pods -n keda
```

**4. Aplicar los manifiestos** (namespace primero; luego el resto, el ScaledObject al final)
```powershell
kubectl apply -f k8s/
kubectl rollout status deploy/rabbitmq -n ventas
kubectl rollout status deploy/postgres -n ventas
kubectl get pods -n ventas
```
Si `kubectl apply -f k8s/` falla por el orden, aplicar los archivos uno por uno en este orden: namespace, secretos, RabbitMQ, PostgreSQL, generador, procesador, web, ScaledObject.

**5. Verificar KEDA y el HPA**
```powershell
kubectl get scaledobject -n ventas        # READY=True
kubectl get hpa -n ventas                 # TARGETS 0/100, REPLICAS 1
```
`ACTIVE=False` es normal con la cola vacía.

**6. Abrir la web:** http://localhost:30080

**7. Comprobar comunicación interna**
```powershell
kubectl get endpoints -n ventas
kubectl logs -n ventas deploy/procesador --tail=5
```

---

## 6. Ejecución de pruebas

Dejar el sistema en cero antes de cada prueba:
```powershell
kubectl exec -n ventas deploy/postgres -- psql -U ventas_user -d ventasdb -c "TRUNCATE ventas;"
```
y pulsar **Reiniciar generador** en la web (0 enviadas, 0 pendientes).

### A/B/D/E - Corrida completa 1 -> 2 -> 1 (10 000 ventas, ~42 min)

Perfil automático: 300 ventas a 3/s, 3 000 a 15/s, 6 700 a 3/s.

```powershell
powercfg /change standby-timeout-ac 0      # evitar suspensión; laptop enchufada

# Terminal A: registro en CSV (evidencias\elasticidad.csv)
powershell -ExecutionPolicy Bypass -File scripts\registrar_elasticidad.ps1
# Terminal B: réplicas deseadas por el HPA
kubectl get hpa -n ventas -w
# Terminal C: pods procesadores
kubectl get pods -n ventas -l app=procesador -w
```
En la web pulsar **Inicio** (perfil automático) y **no escalar manualmente** durante la prueba. Al terminar (Generador finalizado, 10 000 únicas, pendientes 0, totales con ✓): detener A, B y C con Ctrl+C y ejecutar:
```powershell
kubectl describe hpa keda-hpa-procesador-scaler -n ventas
kubectl exec -n ventas deploy/postgres -- psql -U ventas_user -d ventasdb -c "SELECT procesador, count(*) FROM ventas GROUP BY 1 ORDER BY 1;"
powercfg /change standby-timeout-ac 15     # restaurar suspensión
```
Resultado esperado: al menos dos procesadores distintos y suma de 10 000.

### C - Persistencia
```powershell
kubectl get pvc -n ventas
kubectl exec -n ventas deploy/postgres -- psql -U ventas_user -d ventasdb -c "SELECT count(*) FROM ventas;"
kubectl delete pod -n ventas -l app=postgres
kubectl rollout status deploy/postgres -n ventas
kubectl exec -n ventas deploy/postgres -- psql -U ventas_user -d ventasdb -c "SELECT count(*) FROM ventas;"
```
El conteo debe ser idéntico antes y después. **No** eliminar el PVC (solo se explica: eliminar el PVC o el namespace, resetear Kubernetes en Docker Desktop o perder el disco destruye los datos).

### F - Recuperación de tareas

Con 15 ventas/s activas y la cola cargada (más de 100 mensajes), eliminar **un** procesador sin cierre ordenado:
```powershell
$p = kubectl get pods -n ventas -l app=procesador -o name | Select-Object -First 1
kubectl logs -n ventas $p --tail=2
kubectl delete $p -n ventas --grace-period=0 --force
kubectl logs -n ventas -l app=procesador --prefix --tail=-1 | Select-String "reentrega=True"
```
Debe aparecer una línea con `reentrega=True` (evidencia: venta V000194 reentregada al pod `dc24b`). Luego pausar, esperar cola en 0 y verificar `filas = unicas` y que coincide con las enviadas confirmadas.

### G - Control de duplicados

Con las 10 000 ventas cargadas, reenviar 100 ventas del mismo archivo desde la web.
```powershell
kubectl exec -n ventas deploy/postgres -- psql -U ventas_user -d ventasdb -c "SELECT count(*) AS filas, count(DISTINCT id_venta) AS unicas, sum(importe_centimos) AS total FROM ventas;"
kubectl exec -n ventas deploy/postgres -- psql -U ventas_user -d ventasdb -c "\d ventas"
```
Ejecutar la primera consulta antes y después del reenvío: los valores deben ser idénticos. En los logs del procesador se ven líneas `DUPLICADA(ignorada)`.

### H - Scheduling y recursos

Pod de prueba con `cpu: "100"` (mayor que lo asignable por el nodo):
```powershell
kubectl describe node docker-desktop | Select-String -Pattern "Allocatable" -Context 0,6
kubectl apply -f k8s/pod-pending.yaml
kubectl get pod prueba-pending -n ventas        # Pending
kubectl describe pod prueba-pending -n ventas   # FailedScheduling: Insufficient cpu
kubectl delete pod prueba-pending -n ventas
# corregir el request a 100m en el YAML y volver a aplicar
kubectl apply -f k8s/pod-pending.yaml
kubectl get pod prueba-pending -n ventas        # Running
kubectl delete pod prueba-pending -n ventas     # limpieza
```
Realizar esta prueba **después** de demostrar la elasticidad.

---

## 7. Validación de resultados

Totales esperados (cálculo independiente sobre `datos/ventas.csv`) frente a la base de datos:
```powershell
kubectl exec -n ventas deploy/postgres -- psql -U ventas_user -d ventasdb -c "SELECT count(*) AS filas, count(DISTINCT id_venta) AS unicas, sum(importe_centimos) AS total FROM ventas;"
kubectl exec -n ventas deploy/postgres -- psql -U ventas_user -d ventasdb -c "SELECT sucursal, categoria, count(*), sum(importe_centimos) FROM ventas GROUP BY 1,2 ORDER BY 1,2;"
kubectl exec -n ventas deploy/rabbitmq -- rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
```
Criterios: 10 000 ventas únicas, importe general y totales por sucursal y categoría iguales a la referencia, cola en 0 listos y 0 sin confirmar.

Nota: los logs de los procesadores muestran la hora en UTC (Lima es UTC-5).

---

## 8. Limitaciones observadas

- **Una sola laptop, un solo nodo y un solo volumen:** la recuperación de un pod no da disponibilidad ante la pérdida de la laptop o su disco.
- **RabbitMQ sin volumen persistente:** si su pod se recrea, la cola se pierde.
- **Readiness probe de RabbitMQ:** con timeout de 1 s fallaba bajo carga (188 eventos), dejaba el Service sin endpoints y provocaba pérdidas intermitentes de métrica en KEDA y en la web. Se corrigió con `timeoutSeconds: 10`, `periodSeconds: 15`, `failureThreshold: 6`.
- **Secretos en texto plano** en el manifiesto (`rabbitmq-credenciales`): aceptable en laboratorio; en producción se usaría un gestor de secretos. No subir este proyecto a un repositorio público.
- La demora de 0,2 s por venta es didáctica y no mide potencia de CPU.

---

## 9. Limpieza del entorno

```powershell
kubectl delete namespace ventas        # elimina todos los recursos y el PVC (borra los datos)
helm uninstall keda -n keda            # opcional
kubectl delete namespace keda          # opcional
powercfg /change standby-timeout-ac 15 # restaurar suspensión si no se hizo
```

---

## 10. Referencias

- KEDA: https://keda.sh/docs/ (ScaledObject specification, RabbitMQ Queue)
- Kubernetes: https://kubernetes.io/docs/ (Horizontal Pod Autoscaling, Scheduling)

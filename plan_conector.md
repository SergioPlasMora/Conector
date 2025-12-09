# Plan de Implementación: Componente Conector

**Proyecto:** SaaS de Analítica Descentralizada  
**Versión:** 1.0  
**Fecha:** 09 de Diciembre, 2025  
**Estado Base:** ~60% implementado

---

## 1. Resumen del Estado Actual

### ✅ Funcionalidades Completadas

| Funcionalidad | Archivo | Descripción |
|---------------|---------|-------------|
| Servicio Windows | `service.py` | Implementado con `win32serviceutil`, control de inicio/detención |
| Conexión SSE | `service.py` | `SSEClient` con reconexión automática exponencial |
| Identificación MAC | `service.py` | `NetworkUtils.get_mac_address()` normalizada |
| Heartbeat/Ping | `service.py` | Envío periódico cada 60 segundos |
| Recepción de comandos | `service.py` | Procesa eventos SSE con payloads JSON |
| Modo de prueba | `service.py` | Flag `--test` para ejecución standalone |

### ❌ Funcionalidades Faltantes

| Funcionalidad | Prioridad | Descripción |
|---------------|-----------|-------------|
| Configuración dinámica | Alta | Archivo `config.yaml` en lugar de URLs hardcodeadas |
| Lectura de archivos estáticos | Alta | Servir archivos pre-generados (Parquet, JSON) |
| Traceability IDs | Alta | Propagar `request_id` en toda la cadena |
| Timestamps de alta precisión | Media | t2 (recepción), t3 (inicio envío) |
| Logs estructurados JSON | Media | Salida parseable para análisis |
| Simulación de latencia | Media | `processing_delay` configurable |
| Concurrencia mejorada | Baja | Procesar múltiples comandos en paralelo |

---

## 2. Arquitectura Propuesta

```
┌─────────────────────────────────────────────────────────────┐
│                    CONECTOR (service.py)                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐  │
│  │   Config    │◄───│   Main      │───►│   Logger        │  │
│  │   Manager   │    │   Service   │    │   (JSON/File)   │  │
│  └─────────────┘    └──────┬──────┘    └─────────────────┘  │
│                            │                                │
│         ┌──────────────────┼──────────────────┐             │
│         ▼                  ▼                  ▼             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐  │
│  │   SSE       │    │   Command   │    │   DataSet       │  │
│  │   Handler   │───►│   Executor  │───►│   Reader        │  │
│  └─────────────┘    └─────────────┘    └─────────────────┘  │
│                            │                                │
│                            ▼                                │
│                     ┌─────────────┐                         │
│                     │   Result    │                         │
│                     │   Sender    │                         │
│                     └─────────────┘                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Cambios Propuestos

### 3.1. Configuración Dinámica

#### [NEW] `config.yaml`

```yaml
# Configuración del Conector
enrutador:
  base_url: "http://localhost:8000"
  sse_timeout: 30
  api_timeout: 5

heartbeat:
  interval_seconds: 60

retry:
  max_attempts: 5
  base_delay_seconds: 2
  max_delay_seconds: 10

datasets:
  path: "./datasets"  # Carpeta con archivos pre-generados
  
simulation:
  processing_delay_ms: 0  # Simular latencia de generación

logging:
  level: "INFO"
  format: "json"  # "text" o "json"
  file_path: "C:\\Temp\\Conector_poc.log"

tracing:
  enabled: true
```

#### [NEW] `config_manager.py`

Nuevo módulo para cargar y validar configuración:

```python
# Responsabilidades:
# - Cargar config.yaml al inicio
# - Proveer valores por defecto
# - Exponer configuración de forma tipada
# - Permitir override vía variables de entorno
```

---

### 3.2. Lectura de DataSets Estáticos

#### [NEW] `dataset_reader.py`

```python
# Responsabilidades:
# - Localizar archivos en la carpeta de datasets
# - Leer archivos Parquet, JSON, CSV
# - Soportar streaming por chunks para archivos grandes
# - Reportar tamaño y metadata del archivo
```

**Archivos de prueba a generar:**
- `dataset_1kb.json` — Prueba de latencia mínima
- `dataset_100kb.json` — Carga ligera
- `dataset_1mb.parquet` — Carga media
- `dataset_10mb.parquet` — Carga pesada
- `dataset_100mb.parquet` — Límite superior

---

### 3.3. Sistema de Trazabilidad

#### [MODIFY] `service.py`

Agregar propagación de `request_id` y timestamps:

```python
# Estructura del comando esperado:
{
    "command": "get_dataset",
    "request_id": "uuid-v4",
    "dataset_name": "dataset_1mb.parquet",
    "mac_address": "aa-bb-cc-dd-ee-ff"
}

# Estructura de respuesta:
{
    "request_id": "uuid-v4",
    "mac_address": "aa-bb-cc-dd-ee-ff",
    "status": "success",
    "timestamps": {
        "t2_received": 1702134567.123456789,  # Nanosegundos
        "t3_start_send": 1702134567.234567890
    },
    "data_size_bytes": 1048576,
    "data": "<base64 o streaming>"
}
```

---

### 3.4. Logging Estructurado

#### [MODIFY] `service.py` → `ServiceLogger`

Extender para soportar formato JSON:

```python
# Ejemplo de log JSON:
{
    "timestamp": "2025-12-09T16:02:51.123456789Z",
    "level": "INFO",
    "component": "Conector",
    "request_id": "uuid-v4",
    "event": "COMMAND_RECEIVED",
    "mac_address": "aa-bb-cc-dd-ee-ff",
    "details": {
        "command_type": "get_dataset",
        "dataset_name": "dataset_1mb.parquet"
    }
}
```

---

### 3.5. Simulación de Latencia

#### [MODIFY] `service.py` → `execute_command`

Agregar delay configurable antes de procesar:

```python
async def execute_command(self, command: dict):
    # Simular tiempo de procesamiento (ej. query SQL compleja)
    delay_ms = self.config.simulation.processing_delay_ms
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000)
    
    # Proceder con lectura del dataset
    ...
```

---

## 4. Estructura de Archivos Final

```
Conector/
├── config.yaml                 # [NEW] Configuración dinámica
├── config_manager.py           # [NEW] Cargador de configuración
├── dataset_reader.py           # [NEW] Lector de archivos estáticos
├── service.py                  # [MODIFY] Servicio principal mejorado
├── requirements.txt            # [MODIFY] Agregar PyYAML, pyarrow
├── datasets/                   # [NEW] Carpeta de datos de prueba
│   ├── dataset_1kb.json
│   ├── dataset_100kb.json
│   ├── dataset_1mb.parquet
│   ├── dataset_10mb.parquet
│   └── dataset_100mb.parquet
└── tests/                      # [NEW] Pruebas unitarias
    ├── test_config.py
    ├── test_dataset_reader.py
    └── test_service.py
```

---

## 5. Plan de Ejecución

### Fase 1: Configuración (Estimado: 2-3 horas)

| # | Tarea | Archivos | Complejidad |
|---|-------|----------|-------------|
| 1.1 | Crear `config.yaml` con valores por defecto | `config.yaml` | Baja |
| 1.2 | Implementar `ConfigManager` | `config_manager.py` | Media |
| 1.3 | Integrar ConfigManager en `service.py` | `service.py` | Media |
| 1.4 | Actualizar `requirements.txt` | `requirements.txt` | Baja |

### Fase 2: Lectura de DataSets (Estimado: 3-4 horas)

| # | Tarea | Archivos | Complejidad |
|---|-------|----------|-------------|
| 2.1 | Crear `DatasetReader` básico | `dataset_reader.py` | Media |
| 2.2 | Agregar soporte Parquet | `dataset_reader.py` | Media |
| 2.3 | Implementar streaming por chunks | `dataset_reader.py` | Alta |
| 2.4 | Generar archivos de prueba | `datasets/*` | Baja |

### Fase 3: Trazabilidad (Estimado: 2-3 horas)

| # | Tarea | Archivos | Complejidad |
|---|-------|----------|-------------|
| 3.1 | Modificar `execute_command` para aceptar `request_id` | `service.py` | Media |
| 3.2 | Agregar timestamps de precisión | `service.py` | Baja |
| 3.3 | Propagar `request_id` en respuestas | `service.py` | Baja |

### Fase 4: Logging y Simulación (Estimado: 2 horas)

| # | Tarea | Archivos | Complejidad |
|---|-------|----------|-------------|
| 4.1 | Extender `ServiceLogger` para JSON | `service.py` | Media |
| 4.2 | Implementar `processing_delay` | `service.py` | Baja |
| 4.3 | Agregar métricas de tamaño de archivo | `service.py` | Baja |

---

## 6. Verificación

### Pruebas Manuales

```bash
# 1. Iniciar en modo prueba
python service.py --test

# 2. Verificar logs en formato JSON
type C:\Temp\Conector_poc.log

# 3. Verificar conexión SSE (el Enrutador debe estar activo)
# Observar: "Conexión SSE establecida correctamente"
```

### Comandos de Prueba

Desde el Enrutador, enviar:

```json
{
    "command": "get_dataset",
    "request_id": "test-uuid-123",
    "dataset_name": "dataset_1kb.json",
    "mac_address": "aa-bb-cc-dd-ee-ff"
}
```

**Resultado esperado:**
- Log con `request_id` visible
- Timestamps t2, t3 registrados
- DataSet leído y enviado de vuelta

---

## 7. Dependencias

### Nuevas dependencias para `requirements.txt`

```text
PyYAML>=6.0          # Lectura de config.yaml
pyarrow>=14.0.0      # Lectura de archivos Parquet
python-json-logger   # Logging estructurado JSON (opcional)
```

---

## 8. Notas de Diseño

> [!IMPORTANT]
> **Principio KISS:** Este plan NO incluye autenticación ni validaciones de negocio complejas, siguiendo los principios del documento original para la fase de "Laboratorio de Arquitectura".

> [!TIP]
> **Concurrencia:** El diseño actual con `threading` es suficiente para la PoC. La migración a `asyncio` completo puede considerarse en una fase posterior.

> [!NOTE]
> **URLs hardcodeadas:** Actualmente el código usa `http://localhost:8000`. Con `config.yaml`, esto será configurable sin modificar código.

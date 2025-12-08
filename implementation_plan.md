# 🏗️ Arquitectura Gateway-API Communication System (PoC)

## Índice
1. [Visión General](#visión-general)
2. [Modelo de Comunicación](#modelo-de-comunicación)
3. [Infraestructura del Servidor](#infraestructura-del-servidor)
4. [Estrategias de Optimización](#estrategias-de-optimización)
5. [Transferencia de Archivos](#transferencia-de-archivos)
6. [Garantías de Entrega](#garantías-de-entrega)
7. [Roadmap](#roadmap)

---

## Visión General

### Objetivo
Sistema de comunicación bidireccional entre API centralizada y Gateways distribuidos para:
- Ejecutar consultas SQL en bases de datos remotas
- Transferir resultados en tiempo real
- Transferir archivos Parquet de gran tamaño (hasta 100+ MB)

### Requisitos

| Aspecto | Requisito |
|---------|-----------|
| **Latencia** | < 3 segundos (100MB) |
| **Concurrencia** | 1,000+ gateways |
| **Garantía entrega** | At-least-once |
| **Seguridad** | Sin puertos abiertos en clientes |

---

## Modelo de Comunicación

### Arquitectura: SSE + POST

```
[API] ──SSE──> [Gateway]     (Comandos)
[Gateway] ──POST──> [API]    (Resultados)
```

> [!IMPORTANT]
> Los Gateways solo hacen conexiones salientes. No hay puertos abiertos en máquinas cliente.

---

## Infraestructura del Servidor

### Servidor (Canadá)

| Componente | Capacidad |
|------------|-----------|
| **CPU** | 6 vCores |
| **RAM** | 12.2 GB |
| **Storage** | 100 GB SSD |
| **Latencia** | 81ms |

### Distribución de Recursos

| Componente | RAM | CPU |
|------------|-----|-----|
| FastAPI (4 workers) | 2-3 GB | 2 cores |
| Redis | 500 MB | 0.5 core |
| MinIO | 500 MB | 0.5 core |
| Reserva | 2 GB | 1 core |

### TCP Tuning

```bash
# /etc/sysctl.conf
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
```

---

## Estrategias de Optimización

### 1. Streaming de Resultados

```python
# ✅ Correcto - Solo 64KB en RAM
async for chunk in request.stream():
    await save_chunk_to_disk(chunk)
```

### 2. Object Storage (MinIO)

```
[Gateway] ──multipart──> [MinIO] ──URL──> [Cliente]
```

| Config | Valor |
|--------|-------|
| Retención | 1 hora |
| Chunk size | 5 MB |

### 3. Compresión

| Algoritmo | Ratio | Velocidad | Recomendado |
|-----------|-------|-----------|-------------|
| **Zstd-1** | 3-4x | ⚡⚡⚡⚡ | ✅ PoC |
| LZ4 | 2-3x | ⚡⚡⚡⚡⚡ | Alternativa |

### 4. Rate Limiting

| Operación | Límite |
|-----------|--------|
| Queries pequeños | 200 sim. |
| Archivos medianos | 50 sim. |
| Archivos grandes | 10 sim. |

### 5. SSE Eficiente

| Parámetro | Valor |
|-----------|-------|
| Heartbeat | 30 seg |
| Timeout | 5 min |
| Workers | 4 |

---

## Transferencia de Archivos

### Flujo (Archivo Grande)

1. Gateway ejecuta query
2. Gateway comprime con Zstd
3. Gateway calcula checksum
4. Gateway divide en chunks (512KB)
5. Gateway envía chunks via POST
6. Servidor almacena en MinIO
7. Servidor genera URL
8. Cliente descarga directo

### Estructura Chunk

```json
{
  "query_id": "uuid",
  "chunk_index": 0,
  "total_chunks": 50,
  "checksum": "xxhash64",
  "data": "binary"
}
```

---

## Garantías de Entrega

### At-Least-Once

**Servidor:**
- Persistir comando antes de enviar
- Timeout 30s, máximo 3 reintentos

**Gateway:**
- Reintentos con backoff exponencial
- Máximo 10 reintentos por chunk

---

## Roadmap

### PoC (Actual)
- [x] Conexión SSE básica
- [x] Envío de comandos
- [ ] Streaming de resultados
- [ ] Compresión Zstd
- [ ] Chunking
- [ ] MinIO

### Producción (Futuro)
- [ ] Rate limiting
- [ ] At-least-once delivery
- [ ] Monitoreo
- [ ] Horizontal scaling

---

*Versión: 1.0 (PoC)*

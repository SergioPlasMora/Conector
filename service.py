"""
Servicio Conector PoC - Sistema de Comunicación de Datos Distribuidos
Versión actualizada con configuración dinámica, lectura de DataSets y trazabilidad.
"""
import win32serviceutil
import win32service
import win32event
import servicemanager
import logging
import time
import threading
import requests
import sys
import os
import json
import psutil
import uuid
from datetime import datetime, timezone
from sseclient import SSEClient

# Importar módulos del proyecto
from config_manager import get_config, Config, get_ws_config, WebSocketConfig
from dataset_reader import DatasetReader, DatasetResult
from transport import TransportClient, SSETransportClient, WSTransportClient


class JSONFormatter(logging.Formatter):
    """Formateador de logs en JSON estructurado."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "component": "Conector",
            "message": record.getMessage(),
        }
        
        # Agregar campos extra si existen
        if hasattr(record, "request_id") and record.request_id:
            log_entry["request_id"] = record.request_id
        if hasattr(record, "mac_address") and record.mac_address:
            log_entry["mac_address"] = record.mac_address
        if hasattr(record, "extra_data") and record.extra_data:
            log_entry["details"] = record.extra_data
        
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """Formateador de logs en texto legible."""
    
    def __init__(self):
        super().__init__(
            fmt="%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )


class ServiceLogger:
    """Manejo centralizado de logging con soporte JSON/Text."""
    
    def __init__(self, config: Config = None):
        if config is None:
            config = get_config()
        
        self.config = config
        log_path = config.logging.file_path
        
        # Asegurarse de que el directorio de logs exista
        log_dir = os.path.dirname(log_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        self.log_path = log_path
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Configuración del logger con formato configurable."""
        logger = logging.getLogger("Conector_poc")
        log_level = getattr(logging, self.config.logging.level.upper(), logging.INFO)
        logger.setLevel(log_level)
        
        if logger.hasHandlers():
            logger.handlers.clear()
        
        # File handler
        file_handler = logging.FileHandler(self.log_path, encoding='utf-8')
        if self.config.logging.format.lower() == "json":
            file_handler.setFormatter(JSONFormatter())
        else:
            file_handler.setFormatter(TextFormatter())
        logger.addHandler(file_handler)
        
        # Console handler (siempre texto para legibilidad)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(TextFormatter())
        logger.addHandler(console_handler)
        
        return logger
    
    def log_boot(self, message: str):
        self.logger.info(f"[BOOT] {message}")
    
    def log_service_event(self, event: str, details: str = ""):
        self.logger.info(f"[SERVICE] {event} - {details}")
    
    def log_with_context(self, level: str, message: str, request_id: str = None, mac_address: str = None, extra_data: dict = None):
        """Log con contexto adicional para trazabilidad."""
        extra = {}
        if request_id:
            extra["request_id"] = request_id
        if mac_address:
            extra["mac_address"] = mac_address
        if extra_data:
            extra["extra_data"] = extra_data
        
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(message, extra=extra if extra else None)


class NetworkUtils:
    """Utilidades de red siguiendo principio de responsabilidad única."""
    
    @staticmethod
    def get_mac_address() -> str:
        """Obtiene la dirección MAC principal del sistema."""
        for iface_name, addrs in psutil.net_if_addrs().items():
            lname = iface_name.lower()
            if "loopback" in lname or "virtual" in lname or "vmware" in lname:
                continue
            
            for addr in addrs:
                if (addr.family == psutil.AF_LINK and 
                    addr.address and 
                    addr.address != "00:00:00:00:00:00"):
                    return addr.address
        
        raw = uuid.getnode()
        return ":".join(f"{(raw >> ele) & 0xFF:02x}" for ele in range(0, 8 * 6, 8)[::-1])
    
    @staticmethod
    def normalize_mac_address(mac: str) -> str:
        """Normaliza formato de MAC address."""
        return mac.lower().replace(":", "-")


class APIClient:
    """Cliente API para comunicación con el Enrutador."""
    
    def __init__(self, config: Config = None):
        if config is None:
            config = get_config()
        
        self.config = config
        self.base_url = config.enrutador.base_url
        self.timeout = config.enrutador.api_timeout
        self.session = requests.Session()
        self.service_logger = ServiceLogger(config)
        self.logger = self.service_logger.logger
    
    def notify_host_active(self, mac_address: str) -> bool:
        """Notifica que el host está activo."""
        url = f"{self.base_url}/hosts/status"
        try:
            response = self.session.patch(
                url,
                params={"mac_address": mac_address, "status": "active"},
                timeout=self.timeout
            )
            return response.status_code == 200
        except requests.RequestException:
            return False
    
    def notify_command_received(self, mac_address: str, command: str) -> bool:
        """Notifica comando recibido."""
        url = f"{self.base_url}/hosts/command_received"
        try:
            payload = {"mac_address": mac_address, "command": command}
            response = self.session.post(url, json=payload, timeout=self.timeout)
            return response.status_code == 200
        except requests.RequestException:
            return False
    
    def send_dataset_result(self, result_payload: dict) -> bool:
        """Envía el resultado de un DataSet al Enrutador."""
        url = f"{self.base_url}/datasets/result"
        try:
            response = self.session.post(url, json=result_payload, timeout=self.timeout)
            return response.status_code == 200
        except requests.RequestException as e:
            self.logger.error(f"Error enviando resultado: {e}")
            return False
    
    # =========================================================================
    # Patrón B: Streaming
    # =========================================================================
    
    def stream_init(self, request_id: str, mac_address: str, dataset_name: str, total_size: int = None, t2_received: float = None) -> bool:
        """Inicializa un stream para el Patrón B."""
        url = f"{self.base_url}/datasets/stream/init/{request_id}"
        try:
            response = self.session.post(url, json={
                "request_id": request_id,
                "mac_address": mac_address,
                "dataset_name": dataset_name,
                "total_size": total_size,
                "t2_received": t2_received
            }, timeout=self.timeout)
            return response.status_code == 200
        except requests.RequestException as e:
            self.logger.error(f"Error iniciando stream: {e}")
            return False
    
    def stream_chunk(self, request_id: str, chunk_index: int, data: str, is_last: bool = False) -> bool:
        """Envía un chunk de datos al Enrutador."""
        url = f"{self.base_url}/datasets/stream/chunk"
        try:
            response = self.session.post(url, json={
                "request_id": request_id,
                "chunk_index": chunk_index,
                "data": data,
                "is_last": is_last
            }, timeout=self.timeout)
            return response.status_code == 200
        except requests.RequestException as e:
            self.logger.error(f"Error enviando chunk: {e}")
            return False
    
    def stream_complete(self, request_id: str, total_chunks: int, total_bytes: int, status: str = "success", t3_start_send: float = None) -> bool:
        """Señala que el stream está completo."""
        url = f"{self.base_url}/datasets/stream/complete"
        try:
            response = self.session.post(url, json={
                "request_id": request_id,
                "total_chunks": total_chunks,
                "total_bytes": total_bytes,
                "status": status,
                "t3_start_send": t3_start_send
            }, timeout=self.timeout)
            return response.status_code == 200
        except requests.RequestException as e:
            self.logger.error(f"Error completando stream: {e}")
            return False


class ConectorService(win32serviceutil.ServiceFramework):
    """Servicio Conector PoC con soporte para DataSets y trazabilidad."""
    
    _svc_name_ = "ConectorServicePoC"
    _svc_display_name_ = "Conector Service PoC"
    _svc_description_ = "Servicio Conector PoC para comunicación SSE y DataSets"

    def __init__(self, args):
        try:
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
            self.is_alive = True
            self.worker_thread = None
            
            # Cargar configuración
            self.config = get_config()
            
            # Inicializar componentes
            self.service_logger = ServiceLogger(self.config)
            self.logger = self.service_logger.logger
            self.api_client = APIClient(self.config)
            self.dataset_reader = DatasetReader(self.config.datasets.path)
            
            self.logger.info("Servicio PoC inicializado correctamente")
            self.logger.info(f"Enrutador URL: {self.config.enrutador.base_url}")
            self.logger.info(f"DataSets path: {self.config.datasets.path}")
            
        except Exception as e:
            with open(r"C:\Temp\Conector_poc_critical_error.log", "a") as f:
                f.write(f"ERROR CRÍTICO EN __init__: {e}\n")
            raise

    def SvcStop(self):
        """Detiene el servicio de forma controlada."""
        try:
            self.logger.info("Iniciando detención del servicio...")
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            self.is_alive = False
            win32event.SetEvent(self.hWaitStop)
            
            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=10)
                if self.worker_thread.is_alive():
                    self.logger.warning("El hilo no se detuvo en el tiempo esperado")
                else:
                    self.logger.info("Hilo principal detenido correctamente")
            
            self.logger.info("Servicio detenido correctamente")
            
        except Exception as e:
            self.logger.error(f"Error durante la detención del servicio: {e}")

    def SvcDoRun(self):
        """Ejecuta el servicio principal."""
        try:
            self.service_logger.log_service_event("INICIADO", "SvcDoRun ejecutándose")
            
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, "Servicio Conector PoC iniciado correctamente")
            )
            
            self.worker_thread = threading.Thread(target=self.main, daemon=True)
            self.worker_thread.start()
            self.logger.info("Hilo principal iniciado")
            
            win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
            self.logger.info("Señal de detención recibida")
            
        except Exception as e:
            self.logger.error(f"Error crítico en SvcDoRun: {e}")
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_ERROR_TYPE,
                servicemanager.PYS_SERVICE_STOPPED,
                (self._svc_name_, f"Error: {e}")
            )

    def execute_command(self, command: dict):
        """
        Ejecuta comandos recibidos del Enrutador.
        Soporta: get_dataset, get_dataset_stream (Patrón B), run_sql_query (legacy).
        """
        try:
            command_type = command.get("command")
            request_id = command.get("request_id")
            mac_address = command.get("mac_address")
            
            self.logger.info(f"Comando recibido: {command_type}", extra={
                "request_id": request_id,
                "mac_address": mac_address
            })

            # Timestamp t2: Conector recibe solicitud
            t2_received = time.time_ns() / 1e9

            if command_type == "get_dataset":
                self._handle_get_dataset(command, t2_received)
            elif command_type == "get_dataset_stream":
                self._handle_get_dataset_stream(command, t2_received)
            elif command_type == "get_dataset_offload":
                self._handle_get_dataset_offload(command, t2_received)
            else:
                self.logger.warning(f"Comando no soportado: {repr(command_type)}")

        except Exception as e:
            self.logger.error(f"Excepción inesperada al ejecutar comando: {e}")
            self.logger.debug(f"Comando que causó la excepción: {command}")
    
    def _handle_get_dataset(self, command: dict, t2_received: float):
        """Maneja el comando get_dataset para leer archivos estáticos."""
        request_id = command.get("request_id")
        mac_address = command.get("mac_address")
        dataset_name = command.get("dataset_name")
        t1_received = command.get("t1_received", 0)
        
        self.logger.info(f"Procesando DataSet: {dataset_name}", extra={
            "request_id": request_id
        })
        
        # Simular delay de procesamiento si está configurado
        delay_ms = self.config.simulation.processing_delay_ms
        if delay_ms > 0:
            self.logger.info(f"Simulando delay de {delay_ms}ms")
            time.sleep(delay_ms / 1000)
        
        # Timestamp t3: Conector inicia envío de datos
        t3_start_send = time.time_ns() / 1e9
        
        # Leer el DataSet
        result: DatasetResult = self.dataset_reader.read_dataset(dataset_name)
        
        # Preparar payload de respuesta
        result_payload = {
            "request_id": request_id,
            "mac_address": mac_address,
            "status": "success" if result.success else "error",
            "data": result.data if result.success else None,
            "data_size_bytes": result.size_bytes if result.success else None,
            "error_message": result.error_message if not result.success else None,
            "timestamps": {
                "t1_received": t1_received,
                "t2_received": t2_received,
                "t3_start_send": t3_start_send
            }
        }
        
        # Enviar resultado al Enrutador
        if self.api_client.send_dataset_result(result_payload):
            self.logger.info(f"Resultado de DataSet enviado correctamente", extra={
                "request_id": request_id,
                "extra_data": {
                    "size_bytes": result.size_bytes,
                    "success": result.success
                }
            })
        else:
            self.logger.error(f"Error enviando resultado de DataSet", extra={
                "request_id": request_id
            })
    
    def _handle_get_dataset_stream(self, command: dict, t2_received: float):
        """
        Patrón B: Maneja el comando get_dataset_stream para envío chunked.
        Envía el DataSet en múltiples chunks en lugar de un solo payload.
        """
        import base64
        from pathlib import Path
        
        request_id = command.get("request_id")
        mac_address = command.get("mac_address")
        dataset_name = command.get("dataset_name")
        # Usar configuración de chunk_size (KB -> bytes)
        chunk_size = self.config.streaming.chunk_size_kb * 1024
        
        self.logger.info(f"[Patrón B] Streaming DataSet: {dataset_name} (chunk_size: {chunk_size // 1024}KB)", extra={
            "request_id": request_id
        })
        
        # Obtener path al archivo
        file_path = Path(self.config.datasets.path) / dataset_name
        
        if not file_path.exists():
            self.logger.error(f"[Patrón B] DataSet no encontrado: {dataset_name}")
            # Notificar error
            self.api_client.stream_complete(request_id, 0, 0, status="error")
            return
        
        try:
            file_size = file_path.stat().st_size
            
            # t3: Inicio de envío
            t3_start_send = time.time_ns() / 1e9
            
            # 1. Inicializar stream (enviar t2 = cuando recibimos el comando)
            if not self.api_client.stream_init(request_id, mac_address, dataset_name, file_size, t2_received=t2_received):
                self.logger.error(f"[Patrón B] Error iniciando stream")
                return
            
            # 2. Enviar archivo en chunks
            chunk_index = 0
            total_bytes = 0
            
            with open(file_path, 'rb') as f:
                while True:
                    chunk_data = f.read(chunk_size)
                    if not chunk_data:
                        break
                    
                    # Codificar en base64 para transmisión JSON
                    chunk_b64 = base64.b64encode(chunk_data).decode('ascii')
                    
                    # Verificar si es el último chunk
                    next_byte = f.read(1)
                    is_last = len(next_byte) == 0
                    if next_byte:
                        f.seek(-1, 1)
                    
                    # Enviar chunk
                    if not self.api_client.stream_chunk(request_id, chunk_index, chunk_b64, is_last):
                        self.logger.error(f"[Patrón B] Error enviando chunk {chunk_index}")
                        break
                    
                    chunk_index += 1
                    total_bytes += len(chunk_data)
            
            # 3. Señalar finalización (enviar t3)
            self.api_client.stream_complete(request_id, chunk_index, total_bytes, status="success", t3_start_send=t3_start_send)
            
            self.logger.info(
                f"[Patrón B] Stream completado: {chunk_index} chunks, {total_bytes} bytes",
                extra={"request_id": request_id}
            )
            
        except Exception as e:
            self.logger.error(f"[Patrón B] Error en streaming: {e}")
            self.api_client.stream_complete(request_id, 0, 0, status="error")
    
    def _handle_get_dataset_offload(self, command: dict, t2_received: float):
        """
        Patrón C: Sube el DataSet a MinIO y envía URL de descarga al Enrutador.
        """
        from pathlib import Path
        from storage_client import StorageClient, MINIO_AVAILABLE
        
        request_id = command.get("request_id")
        mac_address = command.get("mac_address")
        dataset_name = command.get("dataset_name")
        t1_received = command.get("t1_received", 0)
        
        self.logger.info(f"[Patrón C] Subiendo DataSet a MinIO: {dataset_name}", extra={
            "request_id": request_id
        })
        
        if not MINIO_AVAILABLE:
            self.logger.error("[Patrón C] minio package no instalado")
            self._send_offload_error(request_id, mac_address, "minio package not installed")
            return
        
        if not self.config.storage.enabled:
            self.logger.error("[Patrón C] Storage no habilitado en config.yaml")
            self._send_offload_error(request_id, mac_address, "Storage not enabled in config")
            return
        
        file_path = Path(self.config.datasets.path) / dataset_name
        
        if not file_path.exists():
            self.logger.error(f"[Patrón C] DataSet no encontrado: {dataset_name}")
            self._send_offload_error(request_id, mac_address, f"Dataset not found: {dataset_name}")
            return
        
        try:
            # Crear cliente MinIO
            storage = StorageClient(
                endpoint=self.config.storage.endpoint,
                access_key=self.config.storage.access_key,
                secret_key=self.config.storage.secret_key,
                bucket=self.config.storage.bucket,
                secure=self.config.storage.secure,
                url_expiry_hours=self.config.storage.url_expiry_hours
            )
            
            # Timestamp t3: Inicio de upload
            t3_start_send = time.time_ns() / 1e9
            
            # Subir archivo
            result = storage.upload_file(request_id, str(file_path), mac_address)
            
            if not result.success:
                self._send_offload_error(request_id, mac_address, result.error_message)
                return
            
            # Enviar URL al Enrutador
            result_payload = {
                "request_id": request_id,
                "mac_address": mac_address,
                "status": "success",
                "download_url": result.download_url,
                "data_size_bytes": result.size_bytes,
                "timestamps": {
                    "t1_received": t1_received,
                    "t2_received": t2_received,
                    "t3_start_send": t3_start_send
                }
            }
            
            if self.api_client.send_dataset_result(result_payload):
                self.logger.info(
                    f"[Patrón C] URL enviada: {result.size_bytes} bytes",
                    extra={"request_id": request_id}
                )
            else:
                self.logger.error("[Patrón C] Error enviando resultado")
                
        except Exception as e:
            self.logger.error(f"[Patrón C] Error: {e}")
            self._send_offload_error(request_id, mac_address, str(e))
    
    def _send_offload_error(self, request_id: str, mac_address: str, error_message: str):
        """Envía error de offload al Enrutador."""
        result_payload = {
            "request_id": request_id,
            "mac_address": mac_address,
            "status": "error",
            "error_message": error_message
        }
        self.api_client.send_dataset_result(result_payload)

    def handle_sse_connection(self, sse_url: str, mac_address: str) -> bool:
        """Maneja la conexión SSE con el Enrutador."""
        try:
            self.logger.info(f"Conectando a SSE: {sse_url}")
            headers = {"Accept": "text/event-stream"}

            response = requests.get(
                sse_url,
                headers=headers,
                stream=True,
                timeout=self.config.enrutador.sse_timeout
            )

            if response.status_code != 200:
                self.logger.error(f"Error de conexión SSE: {response.status_code}")
                return False

            client = SSEClient(response)
            self.logger.info("Conexión SSE establecida correctamente")

            if self.api_client.notify_host_active(mac_address):
                self.logger.info("Host marcado como ACTIVE")
            else:
                self.logger.warning("No se pudo marcar host como ACTIVE")

            for event in client.events():
                if not self.is_alive:
                    self.logger.info("Detención solicitada, cerrando conexión SSE")
                    break

                if not event.data.strip():
                    continue

                try:
                    data = json.loads(event.data)
                    command = data.get("command")

                    if command:
                        self.logger.info(f"Comando recibido vía SSE: {command.get('command', 'unknown')}")
                        if self.api_client.notify_command_received(mac_address, command):
                            self.logger.info("Comando notificado correctamente")
                            # Ejecutar en hilo separado para no bloquear SSE
                            threading.Thread(
                                target=self.execute_command,
                                args=(command,),
                                daemon=True
                            ).start()
                        else:
                            self.logger.warning("Error al notificar comando")
                    else:
                        # Heartbeat recibido
                        pass

                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON inválido recibido: {event.data} - Error: {e}")
                except Exception as e:
                    self.logger.error(f"Error procesando evento SSE: {e}")

            return True

        except requests.Timeout:
            self.logger.warning("Timeout en conexión SSE (reintentando)")
            return False
        except requests.ConnectionError:
            self.logger.error("Error de conexión SSE")
            return False
        except Exception as e:
            self.logger.error(f"Error inesperado en SSE: {e}")
            return False

    def main(self):
        """Bucle principal del servicio."""
        try:
            self.service_logger.log_service_event("MAIN_LOOP", "Iniciando bucle principal")

            mac = NetworkUtils.get_mac_address()
            normalized_mac = NetworkUtils.normalize_mac_address(mac)
            self.logger.info(f"MAC address: {normalized_mac}")

            # Ping inicial
            try:
                requests.post(
                    f"{self.config.enrutador.base_url}/hosts/ping",
                    json={"mac_address": normalized_mac},
                    timeout=3
                )
                self.logger.info("Ping inicial enviado con éxito")
            except Exception as e:
                self.logger.warning(f"Ping inicial fallido: {e}")

            # Iniciar heartbeat en background
            threading.Thread(target=self.send_heartbeat, daemon=True).start()

            # Conexión SSE con reintentos
            sse_url = f"{self.config.enrutador.base_url}/sse/conector/{normalized_mac}"
            retry_count = 0

            while self.is_alive:
                self.logger.info(f"Intento de conexión SSE #{retry_count + 1}")

                success = self.handle_sse_connection(sse_url, normalized_mac)

                if not success and self.is_alive:
                    retry_count += 1
                    wait_time = min(
                        self.config.retry.base_delay_seconds * retry_count,
                        self.config.retry.max_delay_seconds
                    )
                    self.logger.warning(f"Reintentando en {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    retry_count = 0

            self.logger.info("Bucle principal terminado")

        except Exception as e:
            self.logger.error(f"Error crítico en main(): {e}")
            self.SvcStop()

    def send_heartbeat(self):
        """Envía heartbeat periódico al Enrutador."""
        mac = NetworkUtils.get_mac_address()
        normalized_mac = NetworkUtils.normalize_mac_address(mac)
        payload = {"mac_address": normalized_mac}
        
        # Envío inicial
        try:
            requests.post(
                f"{self.config.enrutador.base_url}/hosts/ping",
                json=payload,
                timeout=3
            )
        except:
            pass

        while self.is_alive:
            time.sleep(self.config.heartbeat.interval_seconds)
            try:
                requests.post(
                    f"{self.config.enrutador.base_url}/hosts/ping",
                    json=payload,
                    timeout=3
                )
                self.logger.debug("Heartbeat enviado")
            except Exception as e:
                self.logger.warning(f"Ping fallido: {e}")


def test_mode():
    """Modo de prueba para debugging (standalone, sin Windows Service)."""
    print("=== MODO PRUEBA ===")
    
    config = get_config()
    service_logger = ServiceLogger(config)
    service_logger.log_boot("Iniciando en modo prueba")
    logger = service_logger.logger
    api_client = APIClient(config)
    dataset_reader = DatasetReader(config.datasets.path)
    is_alive = True
    
    # Mostrar DataSets disponibles
    datasets = dataset_reader.list_datasets()
    logger.info(f"DataSets disponibles: {datasets}")
    
    def send_heartbeat_standalone():
        nonlocal is_alive
        mac = NetworkUtils.get_mac_address()
        normalized_mac = NetworkUtils.normalize_mac_address(mac)
        payload = {"mac_address": normalized_mac}
        
        try:
            requests.post(f"{config.enrutador.base_url}/hosts/ping", json=payload, timeout=3)
        except:
            pass
        
        while is_alive:
            time.sleep(config.heartbeat.interval_seconds)
            try:
                requests.post(f"{config.enrutador.base_url}/hosts/ping", json=payload, timeout=3)
                logger.debug("Heartbeat enviado")
            except Exception as e:
                logger.warning(f"Ping fallido: {e}")
    
    def execute_command_standalone(command: dict):
        """Ejecuta comando en modo standalone."""
        command_type = command.get("command")
        request_id = command.get("request_id")
        mac_address = command.get("mac_address")
        
        logger.info(f"Comando recibido: {command_type}")
        
        t2_received = time.time_ns() / 1e9
        
        if command_type == "get_dataset":
            dataset_name = command.get("dataset_name")
            t1_received = command.get("t1_received", 0)
            
            # Simular delay si está configurado
            delay_ms = config.simulation.processing_delay_ms
            if delay_ms > 0:
                logger.info(f"Simulando delay de {delay_ms}ms")
                time.sleep(delay_ms / 1000)
            
            t3_start_send = time.time_ns() / 1e9
            
            result = dataset_reader.read_dataset(dataset_name)
            
            result_payload = {
                "request_id": request_id,
                "mac_address": mac_address,
                "status": "success" if result.success else "error",
                "data": result.data if result.success else None,
                "data_size_bytes": result.size_bytes if result.success else None,
                "error_message": result.error_message if not result.success else None,
                "timestamps": {
                    "t1_received": t1_received,
                    "t2_received": t2_received,
                    "t3_start_send": t3_start_send
                }
            }
            
            if api_client.send_dataset_result(result_payload):
                logger.info(f"Resultado enviado: {result.size_bytes} bytes, success={result.success}")
            else:
                logger.error("Error enviando resultado")
        
        elif command_type == "get_dataset_stream":
            # Patrón B: Streaming
            import base64
            from pathlib import Path
            
            dataset_name = command.get("dataset_name")
            t2_received = time.time_ns() / 1e9  # t2: Conector recibe
            # Usar configuración de chunk_size (KB -> bytes)
            chunk_size = config.streaming.chunk_size_kb * 1024
            
            logger.info(f"[Patrón B] Streaming DataSet: {dataset_name} (chunk_size: {chunk_size // 1024}KB)")
            
            file_path = Path(config.datasets.path) / dataset_name
            
            if not file_path.exists():
                logger.error(f"[Patrón B] DataSet no encontrado: {dataset_name}")
                api_client.stream_complete(request_id, 0, 0, status="error")
                return
            
            try:
                file_size = file_path.stat().st_size
                
                # t3: Inicio de envío
                t3_start_send = time.time_ns() / 1e9
                
                # 1. Inicializar stream (enviar t2)
                if not api_client.stream_init(request_id, mac_address, dataset_name, file_size, t2_received=t2_received):
                    logger.error("[Patrón B] Error iniciando stream")
                    return
                
                # 2. Enviar archivo en chunks
                chunk_index = 0
                total_bytes = 0
                
                with open(file_path, 'rb') as f:
                    while True:
                        chunk_data = f.read(chunk_size)
                        if not chunk_data:
                            break
                        
                        chunk_b64 = base64.b64encode(chunk_data).decode('ascii')
                        
                        next_byte = f.read(1)
                        is_last = len(next_byte) == 0
                        if next_byte:
                            f.seek(-1, 1)
                        
                        if not api_client.stream_chunk(request_id, chunk_index, chunk_b64, is_last):
                            logger.error(f"[Patrón B] Error enviando chunk {chunk_index}")
                            break
                        
                        chunk_index += 1
                        total_bytes += len(chunk_data)
                
                # 3. Señalar finalización (enviar t3)
                api_client.stream_complete(request_id, chunk_index, total_bytes, status="success", t3_start_send=t3_start_send)
                logger.info(f"[Patrón B] Stream completado: {chunk_index} chunks, {total_bytes} bytes")
                
            except Exception as e:
                logger.error(f"[Patrón B] Error en streaming: {e}")
                api_client.stream_complete(request_id, 0, 0, status="error")
        
        elif command_type == "get_dataset_offload":
            # Patrón C: Offloading a MinIO
            from pathlib import Path
            from storage_client import StorageClient, MINIO_AVAILABLE
            
            dataset_name = command.get("dataset_name")
            t1_received = command.get("t1_received", 0)
            t2_received = time.time_ns() / 1e9  # t2: Conector recibe
            
            logger.info(f"[Patrón C] Subiendo DataSet a MinIO: {dataset_name}")
            
            if not MINIO_AVAILABLE:
                logger.error("[Patrón C] minio package no instalado")
                return
            
            if not config.storage.enabled:
                logger.error("[Patrón C] Storage no habilitado en config.yaml")
                return
            
            file_path = Path(config.datasets.path) / dataset_name
            
            if not file_path.exists():
                logger.error(f"[Patrón C] DataSet no encontrado: {dataset_name}")
                return
            
            try:
                storage = StorageClient(
                    endpoint=config.storage.endpoint,
                    access_key=config.storage.access_key,
                    secret_key=config.storage.secret_key,
                    bucket=config.storage.bucket,
                    secure=config.storage.secure,
                    url_expiry_hours=config.storage.url_expiry_hours
                )
                
                t3_start_send = time.time_ns() / 1e9
                result = storage.upload_file(request_id, str(file_path), mac_address)
                
                if not result.success:
                    logger.error(f"[Patrón C] Error subiendo: {result.error_message}")
                    return
                
                result_payload = {
                    "request_id": request_id,
                    "mac_address": mac_address,
                    "status": "success",
                    "download_url": result.download_url,
                    "data_size_bytes": result.size_bytes,
                    "timestamps": {
                        "t1_received": t1_received,
                        "t2_received": t2_received,
                        "t3_start_send": t3_start_send
                    }
                }
                
                if api_client.send_dataset_result(result_payload):
                    logger.info(f"[Patrón C] URL enviada: {result.size_bytes} bytes")
                else:
                    logger.error("[Patrón C] Error enviando resultado")
                    
            except Exception as e:
                logger.error(f"[Patrón C] Error: {e}")
        
        else:
            logger.warning(f"Comando no soportado: {command_type}")
    
    def handle_sse_connection_standalone(sse_url: str, mac_address: str) -> bool:
        nonlocal is_alive
        try:
            logger.info(f"Conectando a SSE: {sse_url}")
            headers = {"Accept": "text/event-stream"}
            response = requests.get(sse_url, headers=headers, stream=True, timeout=(5, 30))
            
            if response.status_code != 200:
                logger.error(f"Error de conexión SSE: {response.status_code}")
                return False
            
            client = SSEClient(response)
            logger.info("Conexión SSE establecida correctamente")
            
            if api_client.notify_host_active(mac_address):
                logger.info("Host marcado como ACTIVE")
            else:
                logger.warning("No se pudo marcar host como ACTIVE")
            
            for event in client.events():
                if not is_alive:
                    logger.info("Detención solicitada, cerrando conexión SSE")
                    break
                
                if not event.data.strip():
                    continue
                
                try:
                    data = json.loads(event.data)
                    command = data.get("command")
                    if command:
                        logger.info(f"Comando recibido: {command.get('command', 'unknown')}")
                        threading.Thread(
                            target=execute_command_standalone,
                            args=(command,),
                            daemon=True
                        ).start()
                except json.JSONDecodeError as e:
                    logger.error(f"JSON inválido: {event.data} - {e}")
                except Exception as e:
                    logger.error(f"Error procesando evento SSE: {e}")
            
            return True
        except requests.Timeout:
            logger.warning("Timeout en conexión SSE")
            return False
        except requests.ConnectionError:
            logger.error("Error de conexión SSE")
            return False
        except Exception as e:
            logger.error(f"Error inesperado en SSE: {e}")
            return False
    
    # =========================================================================
    # NUEVA FUNCIONALIDAD: Selección de transporte (SSE o WebSocket)
    # =========================================================================
    ws_config = get_ws_config()
    
    def create_transport() -> TransportClient:
        """Factory para crear el cliente de transporte apropiado."""
        if ws_config.enabled:
            logger.info("Usando transporte WebSocket")
            return WSTransportClient(ws_config, logger)
        else:
            logger.info("Usando transporte SSE")
            return SSETransportClient(config, logger)
    
    def handle_transport_connection(transport: TransportClient, mac_address: str) -> bool:
        """Maneja la conexión usando el transporte seleccionado."""
        nonlocal is_alive
        
        if not transport.connect(mac_address):
            return False
        
        # Notificar que el host está activo
        if api_client.notify_host_active(mac_address):
            logger.info("Host marcado como ACTIVE")
        else:
            logger.warning("No se pudo marcar host como ACTIVE")
        
        try:
            while is_alive and transport.is_connected:
                command = transport.receive_command()
                
                if command is None:
                    if not transport.is_connected:
                        logger.warning("Conexión perdida")
                        return False
                    continue
                
                logger.info(f"Comando recibido: {command.get('command', 'unknown')}")
                threading.Thread(
                    target=execute_command_standalone,
                    args=(command,),
                    daemon=True
                ).start()
            
            return True
            
        except Exception as e:
            logger.error(f"Error en conexión de transporte: {e}")
            return False
        finally:
            transport.disconnect()
    
    try:
        mac = NetworkUtils.get_mac_address()
        normalized_mac = NetworkUtils.normalize_mac_address(mac)
        logger.info(f"MAC address: {normalized_mac}")
        logger.info(f"Enrutador URL: {config.enrutador.base_url}")
        logger.info(f"WebSocket habilitado: {ws_config.enabled}")
        
        # Ping inicial
        try:
            requests.post(
                f"{config.enrutador.base_url}/hosts/ping",
                json={"mac_address": normalized_mac},
                timeout=3
            )
            logger.info("Ping inicial enviado con éxito")
        except Exception as e:
            logger.warning(f"Ping inicial fallido: {e}")
        
        # Heartbeat en background
        threading.Thread(target=send_heartbeat_standalone, daemon=True).start()
        
        # Conexión con transporte seleccionado (SSE o WebSocket)
        retry_count = 0
        
        while is_alive:
            transport = create_transport()
            logger.info(f"Intento de conexión #{retry_count + 1} ({transport.transport_type.upper()})")
            
            success = handle_transport_connection(transport, normalized_mac)
            
            if not success and is_alive:
                retry_count += 1
                wait_time = min(
                    config.retry.base_delay_seconds * retry_count,
                    config.retry.max_delay_seconds
                )
                logger.warning(f"Reintentando en {wait_time}s...")
                time.sleep(wait_time)
            else:
                retry_count = 0
        
        logger.info("Bucle principal terminado")
        
    except KeyboardInterrupt:
        print("\nDetenido por usuario")
        is_alive = False
    except Exception as e:
        print(f"Error en modo prueba: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        try:
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(ConectorService)
            servicemanager.StartServiceCtrlDispatcher()
        except Exception as e:
            with open(r'C:\Temp\Conector_poc_dispatch_error.log', 'a') as f:
                f.write(f'CRITICAL: Failed to start service dispatcher: {e}\n')
    else:
        arg = sys.argv[1].lower()
        if arg in ["--test", "test", "debug"]:
            test_mode()
        else:
            win32serviceutil.HandleCommandLine(ConectorService)
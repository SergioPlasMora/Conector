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
from config_manager import get_config, Config
from dataset_reader import DatasetReader, DatasetResult


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
        Soporta: get_dataset (nuevo) y run_sql_query (legacy).
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
            elif command_type == "run_sql_query":
                self._handle_sql_query(command)
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
    
    def _handle_sql_query(self, command: dict):
        """Maneja el comando run_sql_query (legacy)."""
        command_id = command.get("command_id")
        mac_address = command.get("mac_address")
        
        self.logger.info(f"Comando SQL (legacy) recibido: {command_id}")
        
        try:
            response = requests.get(
                f"{self.config.enrutador.base_url}/consultas/commands/{command_id}",
                timeout=self.config.enrutador.api_timeout
            )
            if response.status_code != 200:
                self.logger.error(f"Error al obtener SQL del comando ID {command_id}")
                return

            command_data = response.json()
            sql_query = command_data.get("sql_query")

            if not sql_query:
                self.logger.error(f"El comando ID {command_id} no tiene consulta SQL asociada.")
                return

            self.logger.info(f"Ejecutando SQL:\n{sql_query}")

            from database import execute_query

            try:
                result_list = execute_query(sql_query)
                if result_list is None:
                    raise Exception("Consulta fallida o sin resultados.")

                output = str(result_list)
                status = "success"
                self.logger.info(f"Consulta ejecutada con éxito: {output}")
            except Exception as db_error:
                output = str(db_error)
                status = "error"
                self.logger.error(f"Error al ejecutar SQL: {output}")

            result_payload = {
                "query_id": command_id,
                "mac_address": mac_address,
                "output": output,
                "status": status,
            }

            response = requests.post(
                f"{self.config.enrutador.base_url}/consultas/consultas/results/",
                json=result_payload,
                timeout=self.config.enrutador.api_timeout
            )
            if response.status_code == 201:
                self.logger.info("Resultado SQL enviado correctamente al backend.")
            else:
                self.logger.warning(f"No se pudo enviar resultado SQL. Código: {response.status_code}")

        except requests.RequestException as e:
            self.logger.error(f"Error al obtener la consulta SQL del comando ID {command_id}: {e}")

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
    
    try:
        mac = NetworkUtils.get_mac_address()
        normalized_mac = NetworkUtils.normalize_mac_address(mac)
        logger.info(f"MAC address: {normalized_mac}")
        logger.info(f"Enrutador URL: {config.enrutador.base_url}")
        
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
        
        # Conexión SSE
        sse_url = f"{config.enrutador.base_url}/sse/conector/{normalized_mac}"
        retry_count = 0
        
        while is_alive:
            logger.info(f"Intento de conexión SSE #{retry_count + 1}")
            success = handle_sse_connection_standalone(sse_url, normalized_mac)
            
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
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
from sseclient import SSEClient

class ServiceLogger:
    """Manejo centralizado de logging siguiendo principio DRY"""
    
    def __init__(self, log_path: str = r"C:\Temp\gateway_poc.log"):
        # Asegurarse de que el directorio de logs exista
        log_dir = os.path.dirname(log_path)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        self.log_path = log_path
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Configuración única del logger"""
        logger = logging.getLogger("gateway_poc")
        logger.setLevel(logging.DEBUG)
        
        if logger.hasHandlers():
            logger.handlers.clear()
        
        file_handler = logging.FileHandler(self.log_path)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            "%(levelname)s: %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def log_boot(self, message: str):
        self.logger.info(f"[BOOT] {message}")
    
    def log_service_event(self, event: str, details: str = ""):
        self.logger.info(f"[SERVICE] {event} - {details}")

class NetworkUtils:
    """Utilidades de red siguiendo principio de responsabilidad única"""
    
    @staticmethod
    def get_mac_address() -> str:
        """Obtiene la dirección MAC principal del sistema"""
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
        """Normaliza formato de MAC address"""
        return mac.lower().replace(":", "-")

class APIClient:
    """Cliente API siguiendo principio de responsabilidad única"""   
    def __init__(self, base_url: str = "http://localhost:8000", timeout: int = 5):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.service_logger = ServiceLogger()
        self.logger = self.service_logger.logger
    
    def notify_host_active(self, mac_address: str) -> bool:
        """Notifica que el host está activo"""
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
        """Notifica comando recibido"""
        url = f"{self.base_url}/hosts/command_received"
        try:
            payload = {"mac_address": mac_address, "command": command}
            response = self.session.post(url, json=payload, timeout=self.timeout)
            return response.status_code == 200
        except requests.RequestException:
            return False

class GatewayService(win32serviceutil.ServiceFramework):
    """Servicio Gateway PoC enfocado en SSE y SQL"""
    
    _svc_name_ = "GatewayServicePoC"
    _svc_display_name_ = "Gateway Service PoC"
    _svc_description_ = "Servicio Gateway PoC para comunicación SSE y consultas SQL"

    def __init__(self, args):
        try:
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
            self.is_alive = True
            self.worker_thread = None
            
            self.service_logger = ServiceLogger()
            self.logger = self.service_logger.logger
            self.api_client = APIClient()
            
            self.logger.info("Servicio PoC inicializado correctamente")
            
        except Exception as e:
            with open(r"C:\Temp\gateway_poc_critical_error.log", "a") as f:
                f.write(f"ERROR CRÍTICO EN __init__: {e}\n")
            raise

    def SvcStop(self):
        """Detiene el servicio de forma controlada"""
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
        """Ejecuta el servicio principal"""
        try:
            self.service_logger.log_service_event("INICIADO", "SvcDoRun ejecutándose")
            
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, "Servicio Gateway PoC iniciado correctamente")
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
        Ejecuta comandos CLI limitados a SQL y Run Script (si se desea mantener).
        En esta PoC nos enfocamos en run_sql_query.
        """
        try:
            self.logger.info(f"Comando recibido: {command}")
            command_type = command.get("command")
            self.logger.info(f"Tipo de comando recibido: {repr(command_type)}")

            if command_type == "run_sql_query":
                self.logger.info("Comando reconocido: run_sql_query")
                command_id = command.get("command_id")
                mac_address = command.get("mac_address")

                try:
                    response = requests.get(f"http://localhost:8000/consultas/commands/{command_id}")
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

                    response = requests.post("http://localhost:8000/consultas/consultas/results/", json=result_payload)
                    if response.status_code == 201:
                        self.logger.info("Resultado SQL enviado correctamente al backend.")
                    else:
                        self.logger.warning(f"No se pudo enviar resultado SQL al backend. Código: {response.status_code}")

                except requests.RequestException as e:
                    self.logger.error(f"Error al obtener la consulta SQL del comando ID {command_id}: {e}")

            else:
                self.logger.warning(f"Comando no soportado en esta PoC: {repr(command_type)}")
                return

        except Exception as e:
            self.logger.error(f"Excepción inesperada al ejecutar el comando: {e}")
            self.logger.debug(f"Comando que causó la excepción: {command}")
            
            
    def handle_sse_connection(self, sse_url: str, mac_address: str) -> bool:
        """Maneja la conexión SSE."""
        try:
            self.logger.info(f"Conectando a SSE: {sse_url}")
            headers = {"Accept": "text/event-stream"}

            response = requests.get(
                sse_url,
                headers=headers,
                stream=True,
                timeout=5
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
                        self.logger.info(f"Comando recibido: {command}")
                        if self.api_client.notify_command_received(mac_address, command):
                            self.logger.info("Comando notificado correctamente")
                            self.execute_command(command)
                        else:
                            self.logger.warning("Error al notificar comando")
                    else:
                        self.logger.debug(f"Evento SSE sin comando: {event.data}")

                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON inválido recibido: {event.data} - Error: {e}")
                except Exception as e:
                    self.logger.error(f"Error procesando evento SSE: {e}")

            return True

        except requests.Timeout:
            self.logger.warning("Timeout en conexión SSE (reintentando rápido)")
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

            try:
                requests.post(
                    "http://localhost:8000/hosts/ping",
                    json={"mac_address": normalized_mac},
                    timeout=3
                )
                self.logger.info("Ping inicial enviado con éxito")
            except Exception as e:
                self.logger.warning(f"Ping inicial fallido: {e}")

            threading.Thread(target=self.send_heartbeat, daemon=True).start()

            sse_url = f"http://localhost:8000/sse/gateway/{normalized_mac}"
            retry_count = 0

            while self.is_alive:
                self.logger.info(f"Intento de conexión SSE #{retry_count + 1}")

                success = self.handle_sse_connection(sse_url, normalized_mac)

                if not success and self.is_alive:
                    retry_count += 1
                    wait_time = min(2 * retry_count, 10)
                    self.logger.warning(f"Reintentando en {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    retry_count = 0

            self.logger.info("Bucle principal terminado")

        except Exception as e:
            self.logger.error(f"Error crítico en main(): {e}")
            self.SvcStop()

    def send_heartbeat(self):
        mac = NetworkUtils.get_mac_address()
        normalized_mac = NetworkUtils.normalize_mac_address(mac)

        payload = {"mac_address": normalized_mac}
        
        # Envio inicial
        try:
            requests.post("http://localhost:8000/hosts/ping", json=payload)
        except:
            pass

        while self.is_alive:
            time.sleep(60)
            try:
                requests.post("http://localhost:8000/hosts/ping", json=payload)
                self.logger.info("Heartbeat enviado")
            except Exception as e:
                self.logger.warning("Ping fallido: %s", e)

def test_mode():
    """Modo de prueba para debugging (standalone, sin Windows Service)"""
    print("=== MODO PRUEBA ===")
    service_logger = ServiceLogger()
    service_logger.log_boot("Iniciando en modo prueba")
    logger = service_logger.logger
    api_client = APIClient()
    is_alive = True
    
    def send_heartbeat_standalone():
        nonlocal is_alive
        mac = NetworkUtils.get_mac_address()
        normalized_mac = NetworkUtils.normalize_mac_address(mac)
        payload = {"mac_address": normalized_mac}
        
        try:
            requests.post("http://localhost:8000/hosts/ping", json=payload, timeout=3)
        except:
            pass
        
        while is_alive:
            time.sleep(60)
            try:
                requests.post("http://localhost:8000/hosts/ping", json=payload, timeout=3)
                logger.info("Heartbeat enviado")
            except Exception as e:
                logger.warning("Ping fallido: %s", e)
    
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
                        logger.info(f"Comando recibido: {command}")
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
        
        # Ping inicial
        try:
            requests.post("http://localhost:8000/hosts/ping", json={"mac_address": normalized_mac}, timeout=3)
            logger.info("Ping inicial enviado con éxito")
        except Exception as e:
            logger.warning(f"Ping inicial fallido: {e}")
        
        # Heartbeat en background
        threading.Thread(target=send_heartbeat_standalone, daemon=True).start()
        
        # Conexión SSE
        sse_url = f"http://localhost:8000/sse/gateway/{normalized_mac}"
        retry_count = 0
        
        while is_alive:
            logger.info(f"Intento de conexión SSE #{retry_count + 1}")
            success = handle_sse_connection_standalone(sse_url, normalized_mac)
            
            if not success and is_alive:
                retry_count += 1
                wait_time = min(2 * retry_count, 10)
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
            servicemanager.PrepareToHostSingle(GatewayService)
            servicemanager.StartServiceCtrlDispatcher()
        except Exception as e:
            with open(r'C:\Temp\gateway_poc_dispatch_error.log', 'a') as f:
                f.write(f'CRITICAL: Failed to start service dispatcher: {e}\n')
    else:
        arg = sys.argv[1].lower()
        if arg in ["--test", "test", "debug"]:
            test_mode()
        else:
            win32serviceutil.HandleCommandLine(GatewayService)
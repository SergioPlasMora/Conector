"""
SSE Transport Client - Extraído de service.py original.
Implementa la conexión SSE existente usando la interfaz TransportClient.
"""
import json
import requests
from typing import Optional, Dict, Any, Iterator
from sseclient import SSEClient

from .base import TransportClient
from config_manager import Config


class SSETransportClient(TransportClient):
    """
    Cliente de transporte SSE.
    
    Mantiene una conexión SSE con el Enrutador para recibir comandos.
    Esta es la implementación original extraída de service.py.
    """
    
    def __init__(self, config: Config, logger=None):
        super().__init__(logger)
        self.config = config
        self._response: Optional[requests.Response] = None
        self._client: Optional[SSEClient] = None
        self._events_iterator: Optional[Iterator] = None
    
    @property
    def transport_type(self) -> str:
        return "sse"
    
    def connect(self, mac_address: str) -> bool:
        """
        Establece conexión SSE con el Enrutador.
        
        Args:
            mac_address: MAC address para identificar este Conector
            
        Returns:
            True si la conexión fue exitosa
        """
        self._mac_address = mac_address
        sse_url = f"{self.config.enrutador.base_url}/sse/conector/{mac_address}"
        
        try:
            self.logger.info(f"Conectando a SSE: {sse_url}")
            headers = {"Accept": "text/event-stream"}
            
            self._response = requests.get(
                sse_url,
                headers=headers,
                stream=True,
                timeout=self.config.enrutador.sse_timeout
            )
            
            if self._response.status_code != 200:
                self.logger.error(f"Error de conexión SSE: {self._response.status_code}")
                return False
            
            self._client = SSEClient(self._response)
            self._events_iterator = self._client.events()
            self._is_connected = True
            self.logger.info("Conexión SSE establecida correctamente")
            return True
            
        except requests.Timeout:
            self.logger.warning("Timeout en conexión SSE")
            return False
        except requests.ConnectionError as e:
            self.logger.error(f"Error de conexión SSE: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error inesperado en SSE: {e}")
            return False
    
    def receive_command(self) -> Optional[Dict[str, Any]]:
        """
        Recibe el siguiente comando del Enrutador vía SSE.
        
        Returns:
            Diccionario del comando o None si hay error/desconexión
        """
        if not self._is_connected or self._events_iterator is None:
            return None
        
        try:
            for event in self._events_iterator:
                if not event.data.strip():
                    continue
                
                try:
                    data = json.loads(event.data)
                    command = data.get("command")
                    
                    if command:
                        self.logger.info(f"Comando recibido vía SSE: {command.get('command', 'unknown')}")
                        return command
                    # Heartbeat recibido - continuar esperando
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON inválido recibido: {event.data} - Error: {e}")
                    continue
                    
        except requests.exceptions.ChunkedEncodingError:
            self.logger.warning("Conexión SSE cerrada por el servidor")
            self._is_connected = False
            return None
        except Exception as e:
            self.logger.error(f"Error recibiendo comando SSE: {e}")
            self._is_connected = False
            return None
        
        return None
    
    def disconnect(self) -> None:
        """Cierra la conexión SSE."""
        self._is_connected = False
        
        if self._response:
            try:
                self._response.close()
            except Exception:
                pass
            self._response = None
        
        self._client = None
        self._events_iterator = None
        self.logger.info("Conexión SSE cerrada")

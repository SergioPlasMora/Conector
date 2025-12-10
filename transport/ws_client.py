"""
WebSocket Transport Client - Nueva implementación.
Implementa la conexión WebSocket usando la interfaz TransportClient.
"""
import json
from typing import Optional, Dict, Any

from .base import TransportClient
from config_manager import WebSocketConfig

# websocket-client library
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False


class WSTransportClient(TransportClient):
    """
    Cliente de transporte WebSocket.
    
    Mantiene una conexión WebSocket con el Enrutador para recibir comandos.
    Proporciona comunicación bidireccional más eficiente que SSE.
    """
    
    def __init__(self, ws_config: WebSocketConfig, logger=None):
        super().__init__(logger)
        
        if not WEBSOCKET_AVAILABLE:
            raise ImportError(
                "websocket-client no está instalado. "
                "Ejecuta: pip install websocket-client"
            )
        
        self.ws_config = ws_config
        self._ws: Optional[websocket.WebSocket] = None
    
    @property
    def transport_type(self) -> str:
        return "ws"
    
    def connect(self, mac_address: str) -> bool:
        """
        Establece conexión WebSocket con el Enrutador.
        
        Args:
            mac_address: MAC address para identificar este Conector
            
        Returns:
            True si la conexión fue exitosa
        """
        self._mac_address = mac_address
        ws_url = f"{self.ws_config.url}/{mac_address}"
        
        try:
            self.logger.info(f"Conectando a WebSocket: {ws_url}")
            
            self._ws = websocket.create_connection(
                ws_url,
                timeout=self.ws_config.connection_timeout,
                skip_utf8_validation=True
            )
            
            self._is_connected = True
            self.logger.info("Conexión WebSocket establecida correctamente")
            return True
            
        except websocket.WebSocketTimeoutException:
            self.logger.warning("Timeout en conexión WebSocket")
            return False
        except websocket.WebSocketException as e:
            self.logger.error(f"Error de conexión WebSocket: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error inesperado en WebSocket: {e}")
            return False
    
    def receive_command(self) -> Optional[Dict[str, Any]]:
        """
        Recibe el siguiente comando del Enrutador vía WebSocket.
        
        Returns:
            Diccionario del comando o None si hay error/desconexión
        """
        if not self._is_connected or self._ws is None:
            return None
        
        try:
            # Establecer timeout para el ping
            self._ws.settimeout(self.ws_config.ping_timeout)
            
            # Recibir mensaje
            message = self._ws.recv()
            
            if not message:
                return None
            
            try:
                data = json.loads(message)
                command = data.get("command")
                
                if command:
                    self.logger.info(f"Comando recibido vía WebSocket: {command.get('command', 'unknown')}")
                    return command
                
                # Heartbeat recibido - continuar esperando (recursivo)
                if data.get("heartbeat"):
                    return self.receive_command()
                    
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON inválido recibido: {message} - Error: {e}")
                return None
                
        except websocket.WebSocketTimeoutException:
            # Timeout normal - el servidor no envió nada en el periodo
            # Intentar de nuevo
            return self.receive_command()
        except websocket.WebSocketConnectionClosedException:
            self.logger.warning("Conexión WebSocket cerrada por el servidor")
            self._is_connected = False
            return None
        except Exception as e:
            self.logger.error(f"Error recibiendo comando WebSocket: {e}")
            self._is_connected = False
            return None
    
    def disconnect(self) -> None:
        """Cierra la conexión WebSocket."""
        self._is_connected = False
        
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None
        
        self.logger.info("Conexión WebSocket cerrada")
    
    def send_message(self, message: Dict[str, Any]) -> bool:
        """
        Envía un mensaje al Enrutador (ventaja de WebSocket sobre SSE).
        
        Args:
            message: Diccionario a enviar como JSON
            
        Returns:
            True si el envío fue exitoso
        """
        if not self._is_connected or self._ws is None:
            return False
        
        try:
            self._ws.send(json.dumps(message))
            return True
        except Exception as e:
            self.logger.error(f"Error enviando mensaje WebSocket: {e}")
            return False

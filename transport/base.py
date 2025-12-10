"""
Abstract base class for transport clients (SSE/WebSocket).
Defines the interface that all transport implementations must follow.
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Callable
import logging


class TransportClient(ABC):
    """
    Abstract base class for transport layer clients.
    
    Provides a common interface for SSE and WebSocket implementations,
    allowing the Conector service to switch between transports seamlessly.
    """
    
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger(__name__)
        self._is_connected = False
        self._mac_address: Optional[str] = None
    
    @abstractmethod
    def connect(self, mac_address: str) -> bool:
        """
        Establish connection to the Enrutador.
        
        Args:
            mac_address: MAC address to identify this Conector
            
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def receive_command(self) -> Optional[Dict[str, Any]]:
        """
        Receive the next command from the Enrutador.
        
        This is a blocking call that waits for the next command.
        Returns None if the connection is closed or an error occurs.
        
        Returns:
            Command dictionary or None if connection closed/error
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """
        Close the connection to the Enrutador.
        """
        pass
    
    @property
    def is_connected(self) -> bool:
        """Check if currently connected to the Enrutador."""
        return self._is_connected
    
    @property
    def mac_address(self) -> Optional[str]:
        """Get the MAC address used for this connection."""
        return self._mac_address
    
    @property
    @abstractmethod
    def transport_type(self) -> str:
        """Return the transport type identifier ('sse' or 'ws')."""
        pass

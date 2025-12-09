"""
Gestor de configuración para el Conector.
Carga configuración desde config.yaml con valores por defecto.
"""
import os
import yaml
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path


@dataclass
class EnrutadorConfig:
    """Configuración de conexión al Enrutador."""
    base_url: str = "http://localhost:8000"
    sse_timeout: int = 30
    api_timeout: int = 5


@dataclass
class HeartbeatConfig:
    """Configuración de heartbeat."""
    interval_seconds: int = 60


@dataclass
class RetryConfig:
    """Configuración de reintentos."""
    max_attempts: int = 10
    base_delay_seconds: int = 2
    max_delay_seconds: int = 10


@dataclass
class DatasetsConfig:
    """Configuración de DataSets."""
    path: str = "./datasets"


@dataclass
class SimulationConfig:
    """Configuración de simulación."""
    processing_delay_ms: int = 0


@dataclass
class LoggingConfig:
    """Configuración de logging."""
    level: str = "INFO"
    format: str = "json"
    file_path: str = r"C:\Temp\Conector_poc.log"


@dataclass
class TracingConfig:
    """Configuración de trazabilidad."""
    enabled: bool = True


@dataclass
class StorageConfig:
    """Configuración de almacenamiento MinIO (Patrón C)."""
    enabled: bool = False
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin123"
    bucket: str = "datasets"
    secure: bool = False
    url_expiry_hours: int = 1


@dataclass
class Config:
    """Configuración completa del Conector."""
    enrutador: EnrutadorConfig = field(default_factory=EnrutadorConfig)
    heartbeat: HeartbeatConfig = field(default_factory=HeartbeatConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    datasets: DatasetsConfig = field(default_factory=DatasetsConfig)
    simulation: SimulationConfig = field(default_factory=SimulationConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    tracing: TracingConfig = field(default_factory=TracingConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)


class ConfigManager:
    """
    Gestor de configuración del Conector.
    Carga desde config.yaml, con soporte para variables de entorno.
    """
    
    _instance: Optional['ConfigManager'] = None
    _config: Optional[Config] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._config = self._load_config()
    
    def _find_config_file(self) -> Optional[Path]:
        """Busca el archivo de configuración en ubicaciones conocidas."""
        # Posibles ubicaciones del config.yaml
        locations = [
            Path("config.yaml"),
            Path(__file__).parent / "config.yaml",
            Path(os.environ.get("CONECTOR_CONFIG", "config.yaml")),
        ]
        
        for loc in locations:
            if loc.exists():
                return loc
        
        return None
    
    def _load_config(self) -> Config:
        """Carga la configuración desde archivo o usa valores por defecto."""
        config_file = self._find_config_file()
        
        if config_file is None:
            # Usar valores por defecto si no hay archivo
            return Config()
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            
            return self._parse_config(data)
        except Exception as e:
            print(f"Error cargando config.yaml: {e}. Usando valores por defecto.")
            return Config()
    
    def _parse_config(self, data: dict) -> Config:
        """Parsea el diccionario de configuración a objetos tipados."""
        config = Config()
        
        # Enrutador
        if 'enrutador' in data:
            enr = data['enrutador']
            config.enrutador = EnrutadorConfig(
                base_url=self._get_env_or_value('ENRUTADOR_URL', enr.get('base_url', config.enrutador.base_url)),
                sse_timeout=int(enr.get('sse_timeout', config.enrutador.sse_timeout)),
                api_timeout=int(enr.get('api_timeout', config.enrutador.api_timeout))
            )
        
        # Heartbeat
        if 'heartbeat' in data:
            hb = data['heartbeat']
            config.heartbeat = HeartbeatConfig(
                interval_seconds=int(hb.get('interval_seconds', config.heartbeat.interval_seconds))
            )
        
        # Retry
        if 'retry' in data:
            rt = data['retry']
            config.retry = RetryConfig(
                max_attempts=int(rt.get('max_attempts', config.retry.max_attempts)),
                base_delay_seconds=int(rt.get('base_delay_seconds', config.retry.base_delay_seconds)),
                max_delay_seconds=int(rt.get('max_delay_seconds', config.retry.max_delay_seconds))
            )
        
        # Datasets
        if 'datasets' in data:
            ds = data['datasets']
            config.datasets = DatasetsConfig(
                path=ds.get('path', config.datasets.path)
            )
        
        # Simulation
        if 'simulation' in data:
            sim = data['simulation']
            config.simulation = SimulationConfig(
                processing_delay_ms=int(sim.get('processing_delay_ms', config.simulation.processing_delay_ms))
            )
        
        # Logging
        if 'logging' in data:
            log = data['logging']
            config.logging = LoggingConfig(
                level=log.get('level', config.logging.level),
                format=log.get('format', config.logging.format),
                file_path=log.get('file_path', config.logging.file_path)
            )
        
        # Tracing
        if 'tracing' in data:
            tr = data['tracing']
            config.tracing = TracingConfig(
                enabled=bool(tr.get('enabled', config.tracing.enabled))
            )
        
        # Storage (MinIO)
        if 'storage' in data:
            st = data['storage']
            config.storage = StorageConfig(
                enabled=bool(st.get('enabled', config.storage.enabled)),
                endpoint=st.get('endpoint', config.storage.endpoint),
                access_key=st.get('access_key', config.storage.access_key),
                secret_key=st.get('secret_key', config.storage.secret_key),
                bucket=st.get('bucket', config.storage.bucket),
                secure=bool(st.get('secure', config.storage.secure)),
                url_expiry_hours=int(st.get('url_expiry_hours', config.storage.url_expiry_hours))
            )
        
        return config
    
    def _get_env_or_value(self, env_name: str, default: str) -> str:
        """Obtiene valor de variable de entorno o usa el valor dado."""
        return os.environ.get(env_name, default)
    
    @property
    def config(self) -> Config:
        """Retorna la configuración cargada."""
        return self._config
    
    def reload(self) -> Config:
        """Recarga la configuración desde el archivo."""
        self._config = self._load_config()
        return self._config


# Singleton global para acceso fácil
def get_config() -> Config:
    """Obtiene la configuración global del Conector."""
    return ConfigManager().config


# Alias para importación directa
config = get_config()

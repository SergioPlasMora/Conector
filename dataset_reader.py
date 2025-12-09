"""
Lector de DataSets pre-generados para el Conector.
Soporta lectura de archivos JSON, CSV y Parquet.
"""
import os
import json
import base64
from pathlib import Path
from typing import Optional, Tuple, Any
from dataclasses import dataclass


@dataclass
class DatasetResult:
    """Resultado de lectura de un DataSet."""
    success: bool
    data: Optional[str] = None  # Base64 encoded para binarios, string para texto
    size_bytes: int = 0
    content_type: str = "application/octet-stream"
    error_message: Optional[str] = None


class DatasetReader:
    """
    Lector de DataSets estáticos desde el sistema de archivos.
    """
    
    # Tipos de contenido por extensión
    CONTENT_TYPES = {
        '.json': 'application/json',
        '.csv': 'text/csv',
        '.parquet': 'application/octet-stream',
        '.txt': 'text/plain',
    }
    
    def __init__(self, datasets_path: str = "./datasets"):
        """
        Inicializa el lector de DataSets.
        
        Args:
            datasets_path: Ruta a la carpeta de DataSets
        """
        self.datasets_path = Path(datasets_path)
        self._ensure_path_exists()
    
    def _ensure_path_exists(self):
        """Crea la carpeta de datasets si no existe."""
        if not self.datasets_path.exists():
            self.datasets_path.mkdir(parents=True, exist_ok=True)
    
    def list_datasets(self) -> list[str]:
        """
        Lista todos los DataSets disponibles.
        
        Returns:
            Lista de nombres de archivos disponibles
        """
        if not self.datasets_path.exists():
            return []
        
        datasets = []
        for file in self.datasets_path.iterdir():
            if file.is_file() and file.suffix in self.CONTENT_TYPES:
                datasets.append(file.name)
        
        return sorted(datasets)
    
    def get_dataset_info(self, dataset_name: str) -> Optional[dict]:
        """
        Obtiene información de un DataSet sin leerlo.
        
        Args:
            dataset_name: Nombre del archivo
            
        Returns:
            Diccionario con información o None si no existe
        """
        file_path = self.datasets_path / dataset_name
        
        if not file_path.exists() or not file_path.is_file():
            return None
        
        return {
            "name": dataset_name,
            "size_bytes": file_path.stat().st_size,
            "content_type": self.CONTENT_TYPES.get(
                file_path.suffix.lower(), 
                "application/octet-stream"
            )
        }
    
    def read_dataset(self, dataset_name: str) -> DatasetResult:
        """
        Lee un DataSet completo.
        
        Args:
            dataset_name: Nombre del archivo a leer
            
        Returns:
            DatasetResult con los datos o error
        """
        file_path = self.datasets_path / dataset_name
        
        # Validar que existe
        if not file_path.exists():
            return DatasetResult(
                success=False,
                error_message=f"DataSet '{dataset_name}' no encontrado"
            )
        
        # Validar que es un archivo
        if not file_path.is_file():
            return DatasetResult(
                success=False,
                error_message=f"'{dataset_name}' no es un archivo válido"
            )
        
        try:
            extension = file_path.suffix.lower()
            content_type = self.CONTENT_TYPES.get(extension, "application/octet-stream")
            size_bytes = file_path.stat().st_size
            
            # Leer según el tipo
            if extension == '.json':
                data = self._read_json(file_path)
            elif extension == '.csv':
                data = self._read_text(file_path)
            elif extension == '.parquet':
                data = self._read_binary(file_path)
            else:
                data = self._read_binary(file_path)
            
            return DatasetResult(
                success=True,
                data=data,
                size_bytes=size_bytes,
                content_type=content_type
            )
            
        except Exception as e:
            return DatasetResult(
                success=False,
                error_message=f"Error leyendo DataSet: {str(e)}"
            )
    
    def _read_json(self, file_path: Path) -> str:
        """Lee archivo JSON y retorna como string."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return json.dumps(data, ensure_ascii=False)
    
    def _read_text(self, file_path: Path) -> str:
        """Lee archivo de texto."""
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def _read_binary(self, file_path: Path) -> str:
        """Lee archivo binario y retorna como Base64."""
        with open(file_path, 'rb') as f:
            content = f.read()
        return base64.b64encode(content).decode('ascii')
    
    def read_dataset_chunked(
        self, 
        dataset_name: str, 
        chunk_size: int = 65536
    ):
        """
        Lee un DataSet en chunks para archivos grandes.
        
        Args:
            dataset_name: Nombre del archivo
            chunk_size: Tamaño de cada chunk en bytes
            
        Yields:
            Tuplas de (chunk_data, is_last)
        """
        file_path = self.datasets_path / dataset_name
        
        if not file_path.exists() or not file_path.is_file():
            return
        
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                
                # Verificar si hay más datos
                next_byte = f.read(1)
                is_last = len(next_byte) == 0
                if next_byte:
                    f.seek(-1, 1)  # Retroceder el byte leído
                
                yield (base64.b64encode(chunk).decode('ascii'), is_last)


# Singleton global
_reader_instance: Optional[DatasetReader] = None


def get_dataset_reader(datasets_path: str = "./datasets") -> DatasetReader:
    """Obtiene la instancia global del lector de DataSets."""
    global _reader_instance
    if _reader_instance is None:
        _reader_instance = DatasetReader(datasets_path)
    return _reader_instance

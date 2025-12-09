"""
Cliente de almacenamiento para MinIO/S3.
Usado por el Patrón C (Offloading) para subir DataSets a almacenamiento externo.
"""
import os
import base64
from datetime import timedelta
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    Minio = None
    S3Error = Exception


@dataclass
class UploadResult:
    """Resultado de una operación de upload."""
    success: bool
    download_url: Optional[str] = None
    object_name: Optional[str] = None
    size_bytes: int = 0
    error_message: Optional[str] = None


class StorageClient:
    """
    Cliente de almacenamiento para MinIO/S3.
    
    Permite subir archivos y generar URLs presignadas para descarga directa.
    """
    
    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin123",
        bucket: str = "datasets",
        secure: bool = False,
        url_expiry_hours: int = 1
    ):
        if not MINIO_AVAILABLE:
            raise ImportError("minio package not installed. Run: pip install minio")
        
        self.endpoint = endpoint
        self.bucket = bucket
        self.url_expiry_hours = url_expiry_hours
        
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        
        # Crear bucket si no existe
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Crea el bucket si no existe."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
        except S3Error as e:
            # El bucket podría ya existir
            if "BucketAlreadyOwnedByYou" not in str(e):
                raise
    
    def upload_file(
        self,
        request_id: str,
        file_path: str,
        mac_address: str = ""
    ) -> UploadResult:
        """
        Sube un archivo a MinIO y genera URL presignada.
        
        Args:
            request_id: ID único de la solicitud
            file_path: Path al archivo local
            mac_address: MAC del Conector (para organización)
            
        Returns:
            UploadResult con URL de descarga
        """
        try:
            path = Path(file_path)
            if not path.exists():
                return UploadResult(
                    success=False,
                    error_message=f"Archivo no encontrado: {file_path}"
                )
            
            # Nombre del objeto: request_id/filename
            object_name = f"{request_id}/{path.name}"
            
            # Subir archivo
            file_size = path.stat().st_size
            
            self.client.fput_object(
                self.bucket,
                object_name,
                str(path),
                content_type=self._get_content_type(path.suffix)
            )
            
            # Generar URL presignada
            download_url = self.client.presigned_get_object(
                self.bucket,
                object_name,
                expires=timedelta(hours=self.url_expiry_hours)
            )
            
            return UploadResult(
                success=True,
                download_url=download_url,
                object_name=object_name,
                size_bytes=file_size
            )
            
        except S3Error as e:
            return UploadResult(
                success=False,
                error_message=f"Error S3: {e}"
            )
        except Exception as e:
            return UploadResult(
                success=False,
                error_message=str(e)
            )
    
    def upload_data(
        self,
        request_id: str,
        data: bytes,
        filename: str
    ) -> UploadResult:
        """
        Sube datos en memoria a MinIO.
        
        Args:
            request_id: ID único de la solicitud
            data: Bytes a subir
            filename: Nombre del archivo
            
        Returns:
            UploadResult con URL de descarga
        """
        import io
        
        try:
            object_name = f"{request_id}/{filename}"
            data_stream = io.BytesIO(data)
            data_size = len(data)
            
            self.client.put_object(
                self.bucket,
                object_name,
                data_stream,
                length=data_size,
                content_type=self._get_content_type(Path(filename).suffix)
            )
            
            download_url = self.client.presigned_get_object(
                self.bucket,
                object_name,
                expires=timedelta(hours=self.url_expiry_hours)
            )
            
            return UploadResult(
                success=True,
                download_url=download_url,
                object_name=object_name,
                size_bytes=data_size
            )
            
        except S3Error as e:
            return UploadResult(
                success=False,
                error_message=f"Error S3: {e}"
            )
        except Exception as e:
            return UploadResult(
                success=False,
                error_message=str(e)
            )
    
    def delete_object(self, object_name: str) -> bool:
        """Elimina un objeto del bucket."""
        try:
            self.client.remove_object(self.bucket, object_name)
            return True
        except S3Error:
            return False
    
    def _get_content_type(self, suffix: str) -> str:
        """Determina el content-type según la extensión."""
        content_types = {
            ".json": "application/json",
            ".csv": "text/csv",
            ".parquet": "application/octet-stream",
            ".txt": "text/plain"
        }
        return content_types.get(suffix.lower(), "application/octet-stream")
    
    def health_check(self) -> bool:
        """Verifica conectividad con MinIO."""
        try:
            self.client.list_buckets()
            return True
        except:
            return False

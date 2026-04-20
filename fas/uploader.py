"""
uploader.py — Upload adapters (FTP / REST API) for FAS.
Both return a unified UploadResult. Neither raises — errors are returned.
"""

import ftplib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass
class UploadResult:
    success: bool
    status_code: Optional[int] = None
    remote_path: Optional[str] = None
    latency_ms: float = 0.0
    error: Optional[str] = None


class BaseUploader(ABC):
    @abstractmethod
    def upload(self, file_path: Path, metadata: dict[str, Any]) -> UploadResult: ...


# ---------------------------------------------------------------------------
# FTP
# ---------------------------------------------------------------------------

class FTPUploader(BaseUploader):
    def __init__(self, host: str, user: str, password: str, port: int = 21) -> None:
        self._host, self._user, self._password, self._port = host, user, password, port

    def upload(self, file_path: Path, metadata: dict[str, Any]) -> UploadResult:
        t0 = time.perf_counter()
        try:
            with ftplib.FTP() as ftp:
                ftp.connect(self._host, self._port, timeout=30)
                ftp.login(self._user, self._password)
                ftp.set_pasv(True)
                remote_folder = metadata.get("folder_id")
                if remote_folder:
                    try:
                        ftp.cwd(str(remote_folder))
                    except ftplib.error_perm:
                        pass
                with open(file_path, "rb") as fh:
                    ftp.storbinary(f"STOR {file_path.name}", fh)
            return UploadResult(True, remote_path=file_path.name,
                                latency_ms=(time.perf_counter() - t0) * 1000)
        except Exception as exc:
            return UploadResult(False, latency_ms=(time.perf_counter() - t0) * 1000,
                                error=f"{type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# REST API (requests → urllib fallback)
# ---------------------------------------------------------------------------

class APIUploader(BaseUploader):
    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint

    def upload(self, file_path: Path, metadata: dict[str, Any]) -> UploadResult:
        try:
            return self._via_requests(file_path, metadata)
        except ImportError:
            return self._via_urllib(file_path, metadata)

    def _via_requests(self, file_path: Path, metadata: dict[str, Any]) -> UploadResult:
        import requests
        t0 = time.perf_counter()
        try:
            with open(file_path, "rb") as fh:
                resp = requests.post(
                    self._endpoint,
                    files={"file": (file_path.name, fh)},
                    data={k: str(v) for k, v in metadata.items()},
                    timeout=60,
                )
            ok = 200 <= resp.status_code < 300
            return UploadResult(
                ok, status_code=resp.status_code,
                latency_ms=(time.perf_counter() - t0) * 1000,
                error=None if ok else f"HTTP {resp.status_code}: {resp.text[:200]}",
            )
        except Exception as exc:
            return UploadResult(False, latency_ms=(time.perf_counter() - t0) * 1000,
                                error=f"{type(exc).__name__}: {exc}")

    def _via_urllib(self, file_path: Path, metadata: dict[str, Any]) -> UploadResult:
        import urllib.request
        t0 = time.perf_counter()
        boundary = "FASboundary0000000001"
        try:
            parts: list[bytes] = []
            for k, v in metadata.items():
                parts.append(
                    f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode()
                )
            with open(file_path, "rb") as fh:
                file_data = fh.read()
            parts.append(
                f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
                f"filename=\"{file_path.name}\"\r\nContent-Type: application/octet-stream\r\n\r\n".encode()
                + file_data + b"\r\n"
            )
            parts.append(f"--{boundary}--\r\n".encode())
            body = b"".join(parts)
            req = urllib.request.Request(
                self._endpoint, data=body,
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                status = resp.status
            ok = 200 <= status < 300
            return UploadResult(ok, status_code=status,
                                latency_ms=(time.perf_counter() - t0) * 1000,
                                error=None if ok else f"HTTP {status}")
        except Exception as exc:
            return UploadResult(False, latency_ms=(time.perf_counter() - t0) * 1000,
                                error=f"{type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def build_uploader(profile: Any) -> BaseUploader:
    if profile.upload_type == "ftp":
        ftp = profile.ftp_config
        return FTPUploader(ftp.get("host", ""), ftp.get("user", ""),
                           ftp.get("password", ""), int(ftp.get("port", 21)))
    elif profile.upload_type == "api":
        return APIUploader(profile.api_endpoint)
    raise ValueError(f"Unknown upload type: {profile.upload_type!r}")

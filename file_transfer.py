"""
Module TCP File Transfer đa luồng (Phụ trách: Thành viên C).
Gửi/nhận file nhị phân theo từng chunk, hiển thị progress bar và lưu vào downloads/.
"""

import os
import socket
import threading
from typing import Callable
from config import DOWNLOAD_DIR, CHUNK_SIZE
from chat_engine import recv_exact


class FileTransfer:
    def __init__(self, download_dir: str = DOWNLOAD_DIR):
        self.download_dir = download_dir
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir, exist_ok=True)

    def send_file_async(self, sock: socket.socket, filepath: str, progress_cb: Callable[[str, int, bool, bool], None]):
        """Khởi động luồng gửi file không chặn (Non-blocking file sender)."""
        threading.Thread(target=self._worker_send, args=(sock, filepath, progress_cb), daemon=True).start()

    def _worker_send(self, sock: socket.socket, filepath: str, progress_cb: Callable[[str, int, bool, bool], None]):
        """Đọc và gửi file theo từng chunk nhị phân."""
        if not os.path.exists(filepath):
            progress_cb(filepath, 0, False, True)
            return

        filename = os.path.basename(filepath)
        filesize = os.path.getsize(filepath)

        try:
            # Gửi Header: FILE|<filename>|<filesize>|
            header = f"FILE|{filename}|{filesize}|".encode("utf-8")
            sock.sendall(header)

            sent_bytes = 0
            with open(filepath, "rb") as f:
                while sent_bytes < filesize:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    sock.sendall(chunk)
                    sent_bytes += len(chunk)
                    percent = int((sent_bytes / filesize) * 100)
                    progress_cb(filename, percent, sent_bytes == filesize, False)
        except Exception:
            progress_cb(filename, 0, False, True)

    def receive_file(self, sock: socket.socket, filename: str, filesize: int, progress_cb: Callable[[str, int, bool, bool], None]):
        """Nhận stream file nhị phân và lưu vào thư mục download."""
        save_path = os.path.join(self.download_dir, filename)
        received_bytes = 0

        try:
            with open(save_path, "wb") as f:
                while received_bytes < filesize:
                    remain = filesize - received_bytes
                    read_len = min(remain, CHUNK_SIZE)
                    chunk = sock.recv(read_len)
                    if not chunk:
                        raise ConnectionError("Ket noi bi dut quang khi nhan file.")
                    f.write(chunk)
                    received_bytes += len(chunk)
                    percent = int((received_bytes / filesize) * 100)
                    progress_cb(filename, percent, received_bytes == filesize, False)
            if os.path.getsize(save_path) != filesize:
                raise IOError("Kich thuoc file nhan duoc khong khop.")
        except Exception:
            if os.path.exists(save_path):
                os.remove(save_path)  # Xóa file hỏng nếu truyền lỗi
            progress_cb(filename, 0, False, True)

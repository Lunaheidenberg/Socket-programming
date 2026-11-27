"""
Module TCP File Transfer đa luồng (Phụ trách: Thành viên C).
Gửi/nhận file nhị phân theo từng chunk, hiển thị progress bar và lưu vào downloads/.
"""

import os
import socket
import threading
from typing import Callable, Optional
from config import DOWNLOAD_DIR, CHUNK_SIZE, FILE_PORT_OFFSET
from chat_engine import recv_exact, recv_field


class FileTransfer:
    def __init__(self, my_tcp_port:int, download_dir: str = DOWNLOAD_DIR):
        self.file_port = my_tcp_port + FILE_PORT_OFFSET
        self.download_dir = os.path.abspath(download_dir)   # Chuẩn hoá đường dẫn tuyệt đối
        self.server_sock: Optional[socket.socket] = None
        self.is_running = False

        # Tự động tạo thư mục download nếu chưa tồn tại
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir, exist_ok=True)

        # Callback thông báo UI: cb(filename, percent, is_done, is_error)
        self.on_progress: Optional[Callable[[str, int, bool, bool], None]] = None

    def start_listener(self):
        """Khởi chạy TCP server chuyên lắng nghe kết nối file tới"""
        try:
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_sock.bind(("", self.file_port))
            self.server_sock.listen(5)
            self.is_running = True

            threading.Thread(target=self._acceptor_loop, daemon=True).start()
        except Exception as e:
            print(f"[!] Lỗi khởi tạo File Transfer Listener: {e}")

    def stop(self):
        """Tắt server truyền file"""
        self.is_running = False
        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass

    def send_file_async(self, target_ip:str, target_tcp_port: int, filepath: str):
        """Khởi động luồng riêng để mở kết nối TCP mới dùng cho việc gửi file"""
        target_file_port = target_tcp_port + FILE_PORT_OFFSET
        threading.Thread(
            target=self._worker_send,
            args=(target_ip, target_file_port, filepath),
            daemon=True
        ).start()

    def _worker_send(self, target_ip:str, target_file_port: int, filepath: str):
        """Mở socket, gửi header và file data."""

        # Kiểm tra đường dẫn đến file
        if not os.path.exists(filepath):
            if self.on_progress:
                self.on_progress(filepath, 0, False, True)
            return

        filename = os.path.basename(filepath)
        filesize = os.path.getsize(filepath)
        file_sock = None

        try:
            # Tạo kết nối TCP ngắn hạn để gửi file
            file_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            file_sock.connect((target_ip, target_file_port))

            # Gửi header FILE|<filename>|<filesize>|
            header = f"FILE|{filename}|{filesize}|".encode("utf-8")
            file_sock.sendall(header)

            sent_bytes = 0
            percent = 0
            with open(filepath, "rb") as f:
                while sent_bytes < filesize:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    file_sock.sendall(chunk)
                    sent_bytes += len(chunk)
                    percent = int((sent_bytes / filesize) * 100)
                    if self.on_progress:
                        self.on_progress(filename, percent, sent_bytes == filesize, False)
        except Exception:
            if self.on_progress:
                self.on_progress(filename, percent, False, True)

        finally:
            if file_sock:
                try:
                    file_sock.close()
                except Exception:
                    pass

    def _acceptor_loop(self):
        """Luồng chấp nhận các kết nối gửi file từ đối phương"""
        while self.is_running:
            try:
                conn_sock, _ = self.server_sock.accept()
                threading.Thread(target=self._worker_receive, args=(conn_sock,), daemon=True).start()
            except Exception:
                break

    def _worker_receive(self, sock: socket.socket):
        """Đọc header file và lưu file nhị phân vào thư mục downloads/."""

        # Đọc header file
        try:
            tag_bytes = recv_field(sock, 4)
            if tag_bytes.decode("utf-8") != "FILE":
                return

            filename_bytes = recv_field(sock, 255)
            filesize_bytes = recv_field(sock, 20)
        except Exception:
            return

        filename = filename_bytes.decode("utf-8")
        filesize = int(filesize_bytes)

        # Validate đường dẫn tránh Path Traversal
        if not filename or filename in (".", "..") or "/" in filename or "\\" in filename or filesize < 0:
            raise ValueError("Invalid file header")

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
                    if self.on_progress:
                        self.on_progress(filename, percent, received_bytes == filesize, False)

            if os.path.getsize(save_path) != filesize:
                raise IOError("Kich thuoc file nhan duoc khong khop.")
        except Exception:
            if os.path.exists(save_path):
                os.remove(save_path)  # Xóa file hỏng nếu truyền lỗi
            if self.on_progress:
                self.on_progress(filename, 0, False, True)

        finally:
            try:
                sock.close()
            except Exception:
                pass
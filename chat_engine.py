"""
Module TCP P2P Chat & Framing (Phụ trách: Thành viên A).
Xử lý kết nối TCP, nhận diện tin nhắn framing, quản lý unread count.
"""

import socket
import threading
from typing import Optional, Dict, List, Callable
from peer_manager import PeerInfo
from config import CHUNK_SIZE, MAX_USERNAME_BYTES


def recv_exact(sock: socket.socket, total_bytes: int) -> Optional[bytes]:
    """Helper đọc đủ chính xác số bytes từ TCP stream."""
    buffer = bytearray()
    while len(buffer) < total_bytes:
        try:
            packet = sock.recv(total_bytes - len(buffer))
            if not packet:
                return None
            buffer.extend(packet)
        except (ConnectionResetError, ConnectionRefusedError, BrokenPipeError, TimeoutError, OSError):
            """
            Chỉ bắt những lỗi liên quan đến đứt mạng hoặc đóng kết nối và trả về None, 
            những lỗi khác sẽ khiến chương trình crash và in thông báo ra màn hình.
            """
            return None
    return bytes(buffer)

def recv_field(sock: socket.socket, max_bytes: int) -> bytes:
    data = bytearray()

    while True:
        try:
            ch = sock.recv(1)

            if not ch:
                raise ConnectionError("Connection closed during header")

            if ch == b"|":
                return bytes(data)

            data.extend(ch)

            if len(data) > max_bytes:
                raise ValueError("Field exceeded max allowed size")
        except (ConnectionResetError, BrokenPipeError, TimeoutError) as e:
            raise ConnectionError(f"Network error while reading field: {e}")

class ChatEngine:
    def __init__(self, my_tcp_port: int, my_username: str):
        self.tcp_port = my_tcp_port
        self.my_username = my_username

        self.server_sock: Optional[socket.socket] = None
        self.is_running = False

        self.current_partner: Optional[PeerInfo] = None
        self.current_sock: Optional[socket.socket] = None
        self.conn_lock = threading.Lock()

        # Lưu trữ tin nhắn chưa đọc: peer_name -> list[str]
        self.unread_messages: Dict[str, List[str]] = {}
        self.unread_lock = threading.Lock()

        # Callbacks giao diện
        self.on_message_received: Optional[Callable[[str, str, bool], None]] = None  # (sender, text, is_current)
        self.on_unread_notification: Optional[Callable[[str, int], None]] = None   # (sender, count)

    def start(self) -> bool:
        """Khởi động TCP Server để chờ kết nối đến từ các peer khác."""
        try:
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_sock.bind(("", self.tcp_port))
            self.server_sock.listen(5)
            self.is_running = True

            threading.Thread(target=self._acceptor_loop, daemon=True).start()
            return True
        except (OSError, socket.error) as e:
            print(f"[!] Loi khoi tao TCP Server: {e}")
            return False

    def stop(self):
        """Đóng toàn bộ kết nối và tắt server."""
        self.is_running = False
        self.leave_current_chat()
        if self.server_sock:
            try:
                self.server_sock.close()
            except OSError:
                pass

    def connect_to_peer(self, peer: PeerInfo) -> bool:
        """Chủ động mở kết nối TCP tới một peer khi gõ /chat."""
        self.leave_current_chat()
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)    # Tránh treo vô hạn khi connect
            sock.connect((peer.ip, peer.tcp_port))
            sock.settimeout(None)   # Reset về blocking mode

            id_packet = f"ID|{self.my_username}\n".encode("utf-8")
            sock.sendall(id_packet)

            with self.conn_lock:
                self.current_sock = sock
                self.current_partner = peer

            # Tạo luồng lắng nghe tin nhắn từ peer này
            threading.Thread(target=self._receiver_loop, args=(sock, peer), daemon=True).start()
            return True
        except (ConnectionRefusedError, TimeoutError, OSError):
            if sock:
                try:
                    sock.close()
                except OSError:
                    pass
            return False

    def leave_current_chat(self):
        """Đóng phiên chat hiện tại và gửi cờ BYE|."""
        with self.conn_lock:
            if self.current_sock:
                try:
                    self.current_sock.sendall(b"BYE|\n")
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                finally:
                    try:
                        self.current_sock.close()
                    except OSError:
                        pass
                self.current_sock = None
                self.current_partner = None

    def send_message(self, text: str) -> bool:
        """Gửi tin nhắn định dạng framing MSG|<length>|<text>."""
        with self.conn_lock:
            if not self.current_sock:
                return False
            payload = text.encode("utf-8")
            header = f"MSG|{len(payload)}|".encode("utf-8")
            try:
                self.current_sock.sendall(header + payload)
                return True
            except (BrokenPipeError, ConnectionResetError, OSError):
                return False

    def switch_chat_partner(self, peer_name: str) -> List[str]:
        """Chuyển ngữ cảnh chat và lấy toàn bộ tin nhắn chưa đọc từ peer này."""
        with self.unread_lock:
            return self.unread_messages.pop(peer_name, [])

    def _acceptor_loop(self):
        """Luồng chấp nhận kết nối TCP từ các peer khác (Inbound)."""
        while self.is_running:
            try:
                conn_sock, (client_ip, _) = self.server_sock.accept()
                peer_dummy = PeerInfo("Unknown", client_ip, 0)
                threading.Thread(target=self._receiver_loop, args=(conn_sock, peer_dummy), daemon=True).start()
            except (OSError, socket.error):
                break

    def _receiver_loop(self, sock: socket.socket, peer: PeerInfo):
        """Luồng giải mã framing TCP stream liên tục."""
        try:
            while self.is_running:
                # Đọc phần loại gói tin trước ký tự '|'
                tag = b""
                while not tag.endswith(b"|"):
                    ch = sock.recv(1)
                    if not ch:
                        return
                    tag += ch

                tag_str = tag[:-1].decode("utf-8", errors="ignore")
                if tag_str == "ID":
                    username_bytes = bytearray()
                    while True:
                        ch = sock.recv(1)

                        if not ch:
                            return

                        if ch == b"\n":
                            break

                        username_bytes.extend(ch)

                        if len(username_bytes) > MAX_USERNAME_BYTES:
                            return
                        
                    username = username_bytes.decode("utf-8")
                    if not username:
                        return
                    peer.username = username


                
                elif tag_str == "MSG":
                    # Đọc length
                    len_bytes = b""
                    while not len_bytes.endswith(b"|"):
                        ch = sock.recv(1)
                        if not ch:
                            return
                        len_bytes += ch
                    length = int(len_bytes[:-1].decode("utf-8"))

                    # Đọc chính xác nội dung tin nhắn
                    payload_bytes = recv_exact(sock, length)
                    if payload_bytes is None:
                        return
                    msg_text = payload_bytes.decode("utf-8", errors="ignore")

                    is_current = (self.current_partner and self.current_partner.ip == peer.ip)
                    if is_current:
                        if self.on_message_received:
                            self.on_message_received(peer.username, msg_text, True)
                    else:
                        # Lưu vào unread
                        with self.unread_lock:
                            self.unread_messages.setdefault(peer.username, []).append(msg_text)
                            unread_count = len(self.unread_messages[peer.username])
                        if self.on_unread_notification:
                            self.on_unread_notification(peer.username, unread_count)

                elif tag_str == "BYE":
                    break
        except (ConnectionResetError, BrokenPipeError, ValueError, OSError):
            pass
        finally:
            with self.conn_lock:
                if self.current_sock is sock:
                    self.current_sock = None
                    self.current_partner = None
            try:
                sock.close()
            except OSError:
                pass

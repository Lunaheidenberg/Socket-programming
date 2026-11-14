"""
Module UDP Peer Discovery & Heartbeat (Phụ trách: Thành viên B).
Quản lý gửi presence, lắng nghe trên port 9999 và theo dõi danh sách online/offline.
"""

import socket
import time
import threading
from typing import Dict, List, Optional, Callable
from config import DEFAULT_DISCOVERY_PORT, DISCOVERY_INTERVAL, PEER_TIMEOUT


class PeerInfo:
    """Thông tin đại diện cho một peer trên mạng LAN."""
    def __init__(self, username: str, ip: str, tcp_port: int, status: str = "online"):
        self.username = username
        self.ip = ip
        self.tcp_port = tcp_port
        self.status = status
        self.last_seen = time.time()

    def __repr__(self):
        return f"<Peer {self.username} ({self.ip}:{self.tcp_port}) - {self.status}>"


class PeerManager:
    def __init__(self, username: str, tcp_port: int, discovery_port: int = DEFAULT_DISCOVERY_PORT):
        self.my_username = username
        self.my_tcp_port = tcp_port
        self.discovery_port = discovery_port
        self.my_status = "online"

        self.peers: Dict[str, PeerInfo] = {}  # Key: "ip:tcp_port"
        self.lock = threading.Lock()
        self.is_running = False

        # Callback khi có peer mới online hoặc offline: cb(peer_info, is_online: bool)
        self.on_peer_change: Optional[Callable[[PeerInfo, bool], None]] = None

    def start(self):
        """Khởi động luồng UDP broadcast, listener và luồng timeout check."""
        self.is_running = True
        
        # Luồng gửi broadcast
        threading.Thread(target=self._broadcaster_loop, daemon=True).start()
        # Luồng nhận broadcast
        threading.Thread(target=self._listener_loop, daemon=True).start()
        # Luồng kiểm tra timeout 10 giây
        threading.Thread(target=self._timeout_checker_loop, daemon=True).start()

    def stop(self):
        """Dừng hoạt động của module."""
        self.is_running = False

    def set_status(self, new_status: str):
        """Cập nhật trạng thái người dùng (online / away)."""
        if new_status in ["online", "away"]:
            self.my_status = new_status

    def get_online_peers(self) -> List[PeerInfo]:
        """Lấy danh sách các peers hiện đang online (Thread-safe)."""
        with self.lock:
            return list(self.peers.values())

    def find_peer(self, identifier: str) -> Optional[PeerInfo]:
        """Tìm peer theo số thứ tự (index bắt đầu từ 1) hoặc tên người dùng."""
        with self.lock:
            peer_list = list(self.peers.values())
            if identifier.isdigit():
                idx = int(identifier) - 1
                if 0 <= idx < len(peer_list):
                    return peer_list[idx]
            for peer in peer_list:
                if peer.username == identifier:
                    return peer
        return None

    def _broadcaster_loop(self):
        """Gửi gói tin HELLO|<name>|<tcp_port>|<status> mỗi 3 giây."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        while self.is_running:
            # Format: HELLO|<name>|<tcp_port>|<status>
            message = f"HELLO|{self.my_username}|{self.my_tcp_port}|{self.my_status}".encode("utf-8")
            try:
                sock.sendto(message, ("255.255.255.255", self.discovery_port))
            except Exception:
                pass
            time.sleep(DISCOVERY_INTERVAL)
        sock.close()

    def _listener_loop(self):
        """Lắng nghe gói broadcast UDP trên port 9999."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            # SO_REUSEPORT trên một số nền tảng hỗ trợ
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass

        sock.bind(("", self.discovery_port))

        while self.is_running:
            try:
                data, (sender_ip, _) = sock.recvfrom(1024)
                text = data.decode("utf-8", errors="ignore").strip()
                self._parse_presence(text, sender_ip)
            except Exception:
                break
        sock.close()

    def _parse_presence(self, message: str, sender_ip: str):
        """Phân tích dữ liệu gói HELLO và cập nhật trạng thái peer."""
        parts = message.split("|")
        if len(parts) != 4 or parts[0] != "HELLO":
            return

        username, tcp_port_str, status = parts[1], parts[2], parts[3]
        try:
            tcp_port = int(tcp_port_str)
        except ValueError:
            return

        # Không tự nhận chính mình
        if username == self.my_username and tcp_port == self.my_tcp_port:
            return

        peer_key = f"{sender_ip}:{tcp_port}"
        with self.lock:
            is_new = peer_key not in self.peers
            if is_new:
                peer = PeerInfo(username, sender_ip, tcp_port, status)
                self.peers[peer_key] = peer
                if self.on_peer_change:
                    self.on_peer_change(peer, True)
            else:
                peer = self.peers[peer_key]
                peer.status = status
                peer.last_seen = time.time()

    def _timeout_checker_loop(self):
        """Kiểm tra và xóa peer không gửi tin quá 10 giây."""
        while self.is_running:
            time.sleep(2)
            now = time.time()
            to_remove = []

            with self.lock:
                for key, peer in self.peers.items():
                    if now - peer.last_seen > PEER_TIMEOUT:
                        to_remove.append(key)

                for key in to_remove:
                    peer = self.peers.pop(key)
                    if self.on_peer_change:
                        self.on_peer_change(peer, False)


import socket
import time

from chat_engine import ChatEngine
from peer_manager import PeerInfo

# 1. Chuẩn bị dữ liệu giả
payload = b"Hello from file!"
filename = "test.txt"

# 2. Tạo ChatEngine để xử lý TCP stream
engine = ChatEngine(5001, "Bob")
engine.is_running = True

# 3. Callback giả thay thế FileTransfer
def test_callback(sock, received_filename, received_size):
    received_payload = bytearray()

    while len(received_payload) < received_size:
        chunk = sock.recv(received_size - len(received_payload))

        if not chunk:
            raise ConnectionError("Incomplete file payload")

        received_payload.extend(chunk)

    assert received_filename == filename
    assert received_size == len(payload)
    assert bytes(received_payload) == payload

    print("PASS: FILE header and payload parsed correctly")

engine.on_file_header_received = test_callback

# 4. Tạo hai socket kết nối với nhau
receiver_sock, sender_sock = socket.socketpair()

peer = PeerInfo("Unknown", "127.0.0.1", 5002)

# 5. Khởi động receiver bằng thread
import threading

receiver_thread = threading.Thread(
    target=engine._receiver_loop,
    args=(receiver_sock, peer),
    daemon=True
)
receiver_thread.start()

# 6. Gửi packet giả, giống protocol thật
packet = (
    b"ID|Alice\n"
    + f"FILE|{filename}|{len(payload)}|".encode("utf-8")
    + payload
)

sender_sock.sendall(packet)

# 7. Đợi receiver xử lý
time.sleep(0.5)

sender_sock.close()
engine.is_running = False

receiver_thread.join(timeout=2)

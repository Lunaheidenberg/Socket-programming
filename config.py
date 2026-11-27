"""
Các hằng số cấu hình toàn cục cho hệ thống P2P LAN Chat.
"""

import os

# Cổng UDP Discovery cố định theo đề bài
DEFAULT_DISCOVERY_PORT = 9999

# Chu kỳ gửi tin broadcast (giây)
DISCOVERY_INTERVAL = 3

# Thời gian quá hạn để xóa peer offline (giây)
PEER_TIMEOUT = 10

# Cổng TCP lắng nghe mặc định
DEFAULT_TCP_PORT = 5000

# Độ lệch cổng TCP dùng cho File Transfer (File Port = TCP Port + FILE_PORT_OFFSET)
FILE_PORT_OFFSET = 100

"""
Định vị thư mục nguồn của của ứng dụng để cố định vị trí của thư mục lưu trữ file nhận được
"""
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")

# Kích thước chunk khi truyền nhận file hoặc buffer TCP (bytes)
CHUNK_SIZE = 8192

#giới hạn độ dài username
MAX_USERNAME_BYTES = 64

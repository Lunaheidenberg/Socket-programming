"""
Các hằng số cấu hình toàn cục cho hệ thống P2P LAN Chat.
"""

# Cổng UDP Discovery cố định theo đề bài
DEFAULT_DISCOVERY_PORT = 9999

# Chu kỳ gửi tin broadcast (giây)
DISCOVERY_INTERVAL = 3

# Thời gian quá hạn để xóa peer offline (giây)
PEER_TIMEOUT = 10

# Cổng TCP lắng nghe mặc định
DEFAULT_TCP_PORT = 5000

# Thư mục lưu trữ file nhận được
DOWNLOAD_DIR = "downloads"

# Kích thước chunk khi truyền nhận file hoặc buffer TCP (bytes)
CHUNK_SIZE = 8192

"""
Module Console UI không vỡ dòng (Phụ trách: Thành viên B).
Sử dụng prompt_toolkit kết hợp patch_stdout theo gợi ý mục 4.3 trong đề bài.
"""

import sys
import threading
from typing import List
from prompt_toolkit import prompt
from prompt_toolkit.patch_stdout import patch_stdout
from peer_manager import PeerInfo


class UIConsole:
    def __init__(self):
        self.lock = threading.Lock()

    def print_incoming_message(self, sender: str, text: str):
        """In tin nhắn chat đến mà không làm xé dòng gõ phím."""
        with self.lock:
            print(f"[{sender}] {text}")

    def print_system_notice(self, notice: str):
        """In thông báo hệ thống màu/định dạng chuẩn."""
        with self.lock:
            print(notice)

    def print_error(self, error_msg: str):
        """In thông báo lỗi."""
        with self.lock:
            print(f"[!] {error_msg}")

    def print_file_progress(self, filename: str, percent: int, is_done: bool, is_error: bool):
        """In tiến độ truyền/nhận file."""
        with self.lock:
            if is_error:
                print(f"\n[!] Truyen file {filename} that bai!")
            elif is_done:
                print(f"\n[*] Truyen {filename} (100%) ... Hoan tat.")
            else:
                sys.stdout.write(f"\r[*] Dang truyen {filename}: {percent}%")
                sys.stdout.flush()

    def display_peer_list(self, peers: List[PeerInfo]):
        """Hiển thị danh sách peer trực quan."""
        with self.lock:
            if not peers:
                print("[*] Hien tai khong co peer nao online.")
                return
            print("\n--- DANH SACH PEERS ONLINE ---")
            for idx, peer in enumerate(peers, start=1):
                print(f"  {idx}. {peer.username:<12} ({peer.ip}:{peer.tcp_port}) [{peer.status}]")
            print("------------------------------")

    def read_line(self, prompt_text: str = "> ") -> str:
        """Đọc input người dùng với cơ chế bảo vệ giao diện bằng patch_stdout."""
        with patch_stdout():
            try:
                return prompt(prompt_text)
            except (KeyboardInterrupt, EOFError):
                return "/quit"

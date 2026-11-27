"""
File điều phối chính (Main Entry Point) cho ứng dụng P2P LAN Chat & File Sharing.
"""

import sys
import argparse
from config import DEFAULT_TCP_PORT, DEFAULT_DISCOVERY_PORT
from peer_manager import PeerManager
from chat_engine import ChatEngine
from file_transfer import FileTransfer
from ui_console import UIConsole


def show_help(ui: UIConsole):
    help_text = """
--- DANH MUC LENH HE THONG ---
  /list                 : Xem danh sach peers dang online tren mang LAN
  /chat <name|number>   : Mo ket noi TCP chat voi peer duoc chon
  <noi_dung>            : Gui tin nhan den peer dang chat
  /send <file_path>     : Gui file den peer dang chat
  /leave                : Dong phien chat hien tai
  /status <online|away> : Cap nhat trang thai ca nhan (UDP broadcast)
  /help                 : Xem huong dan
  /quit                 : Thoat ung dung
------------------------------"""
    ui.print_system_notice(help_text)


def main():
    parser = argparse.ArgumentParser(description="P2P LAN Chat & File Sharing (Python)")
    parser.add_argument("--name", type=str, required=True, help="Ten nguoi dung")
    parser.add_argument("--port", type=int, default=DEFAULT_TCP_PORT, help="Cong TCP lang nghe")
    args = parser.parse_args()

    my_name = args.name
    my_port = args.port

    ui = UIConsole()
    peer_manager = PeerManager(my_name, my_port, DEFAULT_DISCOVERY_PORT)
    chat_engine = ChatEngine(my_port, my_name)
    file_transfer = FileTransfer(my_port)

    # 1. Đăng ký Callbacks
    def on_peer_change(peer, is_online):
        if is_online:
            ui.print_system_notice(f"[*] {peer.username} ({peer.ip}) is online.")
        else:
            ui.print_system_notice(f"[*] {peer.username} went offline.")

    peer_manager.on_peer_change = on_peer_change

    def on_message_received(sender, text, is_current):
        if is_current:
            ui.print_incoming_message(sender, text)

    chat_engine.on_message_received = on_message_received

    def on_unread_notification(sender, count):
        ui.print_system_notice(f"[*] New message from {sender} ({count} unread)")

    chat_engine.on_unread_notification = on_unread_notification

    file_transfer.on_progress = lambda filename, percent, is_done, is_error: ui.print_file_progress(filename, percent, is_done, is_error)

    # 2. Khởi động mạng
    if not chat_engine.start():
        ui.print_error(f"Khong the mo TCP Server tren port {my_port}")
        sys.exit(1)
    ui.print_system_notice(f"[*] Listening on TCP port {my_port}")

    file_transfer.start_listener()
    ui.print_system_notice(f"[*] File Transfer Listening on TCP port {file_transfer.file_port}")   

    peer_manager.start()
    ui.print_system_notice(f"[*] Broadcasting presence on UDP port {DEFAULT_DISCOVERY_PORT}")

    # 3. Main Command Loop
    try:
        while True:
            line = ui.read_line("> ").strip()
            if not line:
                continue

            if line.startswith("/"):
                parts = line.split()
                cmd = parts[0]

                if cmd == "/list":
                    peers = peer_manager.get_online_peers()
                    ui.display_peer_list(peers)

                elif cmd == "/status":
                    if len(parts) < 2 or parts[1] not in ["online", "away"]:
                        ui.print_error("Cu phap: /status <online|away>")
                    else:
                        peer_manager.set_status(parts[1])
                        ui.print_system_notice(f"[*] Trang thai da cap nhat: {parts[1]}")

                elif cmd == "/chat":
                    if len(parts) < 2:
                        ui.print_error("Cu phap: /chat <name|number>")
                    else:
                        target_id = parts[1]
                        target_peer = peer_manager.find_peer(target_id)
                        if not target_peer:
                            ui.print_error(f"Khong tim thay peer '{target_id}'. Go /list de kiem tra.")
                        else:
                            ui.print_system_notice(f"[*] Dang ket noi den {target_peer.username} ({target_peer.ip}:{target_peer.tcp_port})...")
                            if chat_engine.connect_to_peer(target_peer):
                                ui.print_system_notice(f"[*] Connected to {target_peer.username} ({target_peer.ip}:{target_peer.tcp_port})")
                                # Đọc và in tin nhắn chờ nếu có
                                unreads = chat_engine.switch_chat_partner(target_peer.username)
                                if unreads:
                                    ui.print_system_notice(f"[*] Switched to {target_peer.username} — {len(unreads)} unread:")
                                    for msg in unreads:
                                        ui.print_incoming_message(target_peer.username, msg)
                            else:
                                ui.print_error(f"Ket noi TCP toi {target_peer.username} that bai.")

                elif cmd == "/leave":
                    if chat_engine.current_partner:
                        partner_name = chat_engine.current_partner.username
                        chat_engine.leave_current_chat()
                        ui.print_system_notice(f"[*] Da dong phien chat voi {partner_name}")
                    else:
                        ui.print_error("Ban khong o trong phien chat nao.")

                elif cmd == "/send":
                    if len(parts) < 2:
                        ui.print_error("Cu phap: /send <file_path>")
                    else:
                        filepath = line[6:].strip()
                        if not chat_engine.current_sock:
                            ui.print_error("Ban phai mo phien /chat truoc khi gui file.")
                        else:
                            target = chat_engine.current_partner
                            ui.print_system_notice(f"[*] Bat dau gui file: {filepath}")
                            file_transfer.send_file_async(target.ip, target.tcp_port, filepath)

                elif cmd == "/help":
                    show_help(ui)

                elif cmd == "/quit":
                    ui.print_system_notice("[*] Dang thoat ung dung...")
                    break
                else:
                    ui.print_error("Lenh khong hop le. Go /help de xem huong dan.")
            else:
                # Gui tin nhan chat
                if chat_engine.current_sock:
                    if chat_engine.send_message(line):
                        ui.print_incoming_message(my_name, line)
                    else:
                        ui.print_error("Gui tin nhan that bai. Ket noi da bi ngat.")
                else:
                    ui.print_error("Ban chua ket noi voi ai. Dung /chat <name|number> de tro chuyen.")
    except (KeyboardInterrupt, EOFError):
        ui.print_system_notice("\n[*] Phat hien ngat ung dung...")
    finally:
        # Cleanup
        peer_manager.stop()
        chat_engine.stop()
        file_transfer.stop()
        ui.print_system_notice("[*] Goodbye!")


if __name__ == "__main__":
    main()

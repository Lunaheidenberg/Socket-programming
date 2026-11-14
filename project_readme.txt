================================================================================
          P2P LAN CHAT & FILE SHARING APPLICATION (COMMAND-LINE)
================================================================================

1. OVERVIEW
--------------------------------------------------------------------------------
This project is a decentralized Peer-to-Peer (P2P) console chat and file sharing 
application built in Python using native Socket and Threading APIs:
  - UDP (Port 9999): Peer discovery, presence broadcasts, and status tracking.
  - TCP: Reliable one-to-one message exchange and chunk-based file transfers.
  - Multi-threaded Console UI: Supports non-blocking input with automatic 
    prompt restoration when incoming messages arrive while typing.

2. REQUIREMENTS & ENVIRONMENT
--------------------------------------------------------------------------------
- Python: Version 3.8 or newer (Python 3.10+ recommended)
- Operating System: Windows 10/11, Linux, or macOS
- Python Packages:
    pip install -r requirements.txt
  (Primarily `prompt_toolkit` for non-blocking console redraw capabilities).

3. PROJECT STRUCTURE
--------------------------------------------------------------------------------
lan-chat-p2p/
├── config.py           - Network ports, timeouts, buffer/chunk size settings
├── peer_manager.py     - UDP Broadcaster, Listener & offline timeout sweeper
├── chat_engine.py      - TCP Acceptor, receiver threads & message framing
├── file_transfer.py    - Async file sender/receiver with progress tracking
├── ui_console.py       - Console display management and prompt patching
├── main.py             - Entrypoint and CLI command loop
├── requirements.txt    - External dependencies
├── README.txt          - Project documentation & setup instructions
└── downloads/          - Target directory for storing received files

4. INSTALLATION & SETUP
--------------------------------------------------------------------------------
Step 1: Open a terminal inside the project directory.
Step 2: (Optional but recommended) Create and activate a virtual environment:
  - On Windows:
      python -m venv venv
      venv\Scripts\activate
  - On Linux / macOS:
      python3 -m venv venv
      source venv/bin/activate

Step 3: Install dependencies:
    pip install -r requirements.txt

5. RUNNING & CLI ARGUMENTS
--------------------------------------------------------------------------------
Syntax:
    python main.py --name <USERNAME> [--port <TCP_PORT>]

Arguments:
  --name : (Required) Your display name in the network.
  --port : (Optional) TCP port to listen on for incoming chats. Default: 5000.

Examples:
  Instance 1: python main.py --name Alice --port 5001
  Instance 2: python main.py --name Bob --port 5002

6. COMMAND CHEAT SHEET
--------------------------------------------------------------------------------
| Command               | Description                                          |
|-----------------------|------------------------------------------------------|
| /list                 | List all active peers discovered on the LAN/Wi-Fi    |
| /chat <name|index>    | Open a TCP chat session with the designated peer     |
| <any normal text>     | Send a text message to the current chat partner      |
| /send <file_path>     | Send a file asynchronously to your chat partner      |
| /leave                | Terminate the current TCP chat session               |
| /status <online|away> | Update your broadcast status                         |
| /help                 | Display list of available commands                   |
| /quit                 | Gracefully exit the application                      |

7. TESTING GUIDELINES
--------------------------------------------------------------------------------

A. Testing Multiple Instances on a Single Machine:
   1. Open two separate command prompt / terminal windows.
   2. Run Peer 1:
        python main.py --name Alice --port 5001
   3. Run Peer 2:
        python main.py --name Bob --port 5002
   4. On Alice's terminal, type `/list` to verify Bob appears in the list.
   5. Type `/chat Bob` to open a TCP session.
   6. Send messages between both windows.
   7. Send a test file:
        /send sample.txt
      Verify that the received file arrives in the `downloads/` directory.

B. Testing Across 2+ Physical Computers (Same LAN / Wi-Fi):
   1. Connect both computers to the same Wi-Fi network or Ethernet switch.
   2. Ensure the firewall allows Python network traffic (see Troubleshooting).
   3. Run on Computer 1:
        python main.py --name Quan --port 5000
   4. Run on Computer 2:
        python main.py --name Minh --port 5000
   5. Verify that each computer discovers the other within 3 to 5 seconds.
   6. Test typing while sending messages from the other machine to confirm
      the prompt does not break or misalign.

8. TROUBLESHOOTING & COMMON PITFALLS
--------------------------------------------------------------------------------
1. Firewall Blocking UDP Broadcasts:
   - On Windows, a Defender pop-up will appear when running for the first time.
     Check BOTH "Private networks" and "Public networks".
   - If peers do not show up via `/list`, temporarily check firewall rules for
     Python or port 9999.

2. Campus / Public Wi-Fi AP Isolation:
   - Many university or coffee shop networks activate "Client Isolation", which
     drops all peer-to-peer packets (both UDP broadcast and direct TCP connections).
   - Solution: Use a phone's Mobile Hotspot or connect both laptops using a direct
     Ethernet cable / ad-hoc connection for demo sessions.

3. "Address already in use" Error:
   - If testing multiple instances on the same machine, make sure each instance is
     given a unique `--port` parameter (e.g., 5001, 5002, 5003).

4. Partial Files in `downloads/`:
   - If a transfer is disconnected mid-way, partial transfers will be cleaned
     up to prevent corrupted files from lingering in the directory.
================================================================================
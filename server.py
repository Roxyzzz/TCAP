import socket
import threading
import time

HOST = "127.0.0.1"
PORT = 5555
ENCODING = "utf-8"
TERMINATOR = "\n"
BUFFER_SIZE = 1024
MAX_LINE = 4096                 # กัน client ส่งข้อมูลยาวไม่จบจนกินหน่วยความจำ
PROTOCOL_ID = "TCAP/1.0"

# ฐานข้อมูลจำลอง (Mock Database) ราคาตลาดของการ์ด
MARKET_DB = {
    "OP01-001": {"price": 300, "name": "Monkey D. Luffy"},
    "OP01-025": {"price": 1200, "name": "Roronoa Zoro (Parallel)"},
    "OP01-060": {"price": 800, "name": "Nami"},
    "OP01-120": {"price": 45000, "name": "Shanks (Manga Rare)"},
    "OP02-004": {"price": 1500, "name": "Ben Beckman"},
    "OP02-013": {"price": 25000, "name": "Kaido (Alt Art)"},
    "OP03-119": {"price": 60000, "name": "Portgas D. Ace (Manga Rare)"},
    "OP04-001": {"price": 500, "name": "Sabo"},
    "OP05-119": {"price": 90000, "name": "Trafalgar Law (Manga Rare)"},
    "OP06-060": {"price": 2000, "name": "Yamato"},
}

# ห้องประมูลที่เปิดอยู่ (closes_at กำหนดตอน server เริ่มทำงาน)
AUCTIONS = {
    "AUC-001": {"card_id": "OP01-120", "bid": 45000, "bidder": "-", "closes_at": 0.0},
    "AUC-002": {"card_id": "OP02-004", "bid": 1200, "bidder": "-", "closes_at": 0.0},
}

STATE_LOCK = threading.Lock()   # ป้องกัน race condition เวลาหลาย thread bid พร้อมกัน
SESSIONS = []                   # session ที่ online อยู่ ใช้ตอน push event
SESSIONS_LOCK = threading.Lock()


class Session:
    """สถานะของ client หนึ่งราย ตลอดช่วงที่ TCP connection ยังเปิดอยู่"""

    def __init__(self, conn, addr):
        self.conn = conn
        self.addr = addr
        self.user_id = None         # ยังไม่ LOGIN
        self.watching = set()       # auction_id ที่สมัครรับ event ไว้
        self.send_lock = threading.Lock()

    def send(self, message):
        """ส่งข้อความหนึ่งบรรทัด ล็อกไว้กัน response กับ push event เขียนซ้อนกัน"""
        with self.send_lock:
            try:
                self.conn.sendall((message + TERMINATOR).encode(ENCODING))
            except OSError:
                pass                # client หลุดไปแล้ว ปล่อยให้ loop หลักจัดการ


def broadcast(auction_id, message, skip_user=None):
    """ส่ง event ให้ทุก session ที่ WATCH ห้องนี้อยู่ ยกเว้นคนที่ระบุใน skip_user"""
    with SESSIONS_LOCK:
        targets = [s for s in SESSIONS
                   if auction_id in s.watching and s.user_id != skip_user]
    for session in targets:
        session.send(message)


def notify_user(user_id, message):
    """ส่ง event ตรงถึงผู้ใช้รายเดียว (ใช้ตอนแจ้งคนที่โดนแซงราคา)"""
    if not user_id or user_id == "-":
        return
    with SESSIONS_LOCK:
        targets = [s for s in SESSIONS if s.user_id == user_id]
    for session in targets:
        session.send(message)


# --------------------------- ตัวจัดการแต่ละคำสั่ง ---------------------------

def cmd_hello(session, args):
    if len(args) != 1:
        return "400 BAD_REQUEST HELLO_NEEDS_VERSION"
    if args[0] != PROTOCOL_ID:
        return f"426 VERSION_NOT_SUPPORTED {args[0]}"
    return f"200 OK {PROTOCOL_ID}"


def cmd_login(session, args):
    if len(args) != 1:
        return "400 BAD_REQUEST LOGIN_NEEDS_USER_ID"
    session.user_id = args[0]
    return f"200 OK {session.user_id}"


def cmd_get_price(session, args):
    if len(args) != 1:
        return "400 BAD_REQUEST GET_PRICE_NEEDS_CARD_ID"
    card_id = args[0]
    card = MARKET_DB.get(card_id)
    if card is None:
        return f"404 NOT_FOUND {card_id}"
    return f"201 PRICE_INFO {card_id} {card['price']} {card['name']}"


def cmd_list_cards(session, args):
    lines = [f"CARD {card_id} {card['price']} {card['name']}"
             for card_id, card in MARKET_DB.items()]
    # ตอบเป็นหลายบรรทัด: บรรทัดแรกบอกจำนวน แล้วตามด้วยรายการทีละบรรทัด
    return [f"205 CARD_LIST {len(lines)}"] + lines


def cmd_list_auctions(session, args):
    now = time.time()
    lines = []
    with STATE_LOCK:
        for auction_id, auction in AUCTIONS.items():
            state = "OPEN" if auction["closes_at"] > now else "CLOSED"
            lines.append(f"AUCTION {auction_id} {auction['card_id']} "
                         f"{auction['bid']} {auction['bidder']} {state}")
    # ตอบเป็นหลายบรรทัด: บรรทัดแรกบอกจำนวน แล้วตามด้วยรายการทีละบรรทัด
    return [f"202 AUCTION_LIST {len(lines)}"] + lines


def cmd_get_auction(session, args):
    if len(args) != 1:
        return "400 BAD_REQUEST GET_AUCTION_NEEDS_AUCTION_ID"
    auction_id = args[0]
    with STATE_LOCK:
        auction = AUCTIONS.get(auction_id)
        if auction is None:
            return f"404 NOT_FOUND {auction_id}"
        left = max(0, int(auction["closes_at"] - time.time()))
        return (f"203 AUCTION_INFO {auction_id} {auction['card_id']} "
                f"{auction['bid']} {auction['bidder']} {left}")


def cmd_place_bid(session, args):
    if session.user_id is None:
        return "401 NOT_LOGGED_IN"
    if len(args) != 2:
        return "400 BAD_REQUEST PLACE_BID_NEEDS_AUCTION_ID_AND_AMOUNT"

    auction_id, raw_amount = args
    if not raw_amount.isdigit() or int(raw_amount) <= 0:
        return f"400 BAD_REQUEST INVALID_AMOUNT {raw_amount}"
    amount = int(raw_amount)

    # ตรวจและอัปเดตราคาภายใน lock เดียวกัน กันสองคน bid พร้อมกันแล้วชนะทั้งคู่
    with STATE_LOCK:
        auction = AUCTIONS.get(auction_id)
        if auction is None:
            return f"404 NOT_FOUND {auction_id}"
        if auction["closes_at"] <= time.time():
            return f"410 AUCTION_CLOSED {auction_id}"
        if amount <= auction["bid"]:
            return f"409 BID_TOO_LOW {auction['bid']}"

        previous_bidder = auction["bidder"]
        auction["bid"] = amount
        auction["bidder"] = session.user_id

    # แจ้งเตือนหลังปล่อย lock แล้ว เพื่อไม่ให้การส่งผ่าน socket ไปหน่วง thread อื่น
    notify_user(previous_bidder, f"900 OUTBID {auction_id} {amount} {session.user_id}")
    broadcast(auction_id, f"901 NEW_BID {auction_id} {amount} {session.user_id}",
              skip_user=session.user_id)
    return f"204 BID_ACCEPTED {auction_id} {amount}"


def cmd_watch(session, args):
    if len(args) != 1:
        return "400 BAD_REQUEST WATCH_NEEDS_AUCTION_ID"
    auction_id = args[0]
    with STATE_LOCK:
        exists = auction_id in AUCTIONS
    if not exists:
        return f"404 NOT_FOUND {auction_id}"
    session.watching.add(auction_id)
    return f"210 WATCHING {auction_id}"


def cmd_unwatch(session, args):
    if len(args) != 1:
        return "400 BAD_REQUEST UNWATCH_NEEDS_AUCTION_ID"
    session.watching.discard(args[0])
    return f"211 UNWATCHED {args[0]}"


HANDLERS = {
    "HELLO": cmd_hello,
    "LOGIN": cmd_login,
    "GET_PRICE": cmd_get_price,
    "LIST_CARDS": cmd_list_cards,
    "LIST_AUCTIONS": cmd_list_auctions,
    "GET_AUCTION": cmd_get_auction,
    "PLACE_BID": cmd_place_bid,
    "WATCH": cmd_watch,
    "UNWATCH": cmd_unwatch,
}


def handle_line(session, line):
    """ประมวลผลหนึ่ง request คืน (ข้อความตอบกลับ, ให้เชื่อมต่อต่อหรือไม่)"""
    parts = line.split()
    command = parts[0].upper()
    args = parts[1:]

    if command == "QUIT":
        return "299 BYE", False

    handler = HANDLERS.get(command)
    if handler is None:
        return f"405 UNKNOWN_COMMAND {command}", True

    try:
        return handler(session, args), True
    except Exception as error:                      # กัน thread ตายเพราะคำสั่งเดียว
        print(f"[ERROR] {session.addr} {command}: {error}")
        return "500 SERVER_ERROR", True


def handle_client(conn, addr):
    print(f"[NEW CONNECTION] เชื่อมต่อกับ {addr} สำเร็จ")
    session = Session(conn, addr)
    with SESSIONS_LOCK:
        SESSIONS.append(session)

    buffer = ""
    try:
        while True:
            chunk = conn.recv(BUFFER_SIZE)
            if not chunk:
                break
            buffer += chunk.decode(ENCODING, errors="replace")

            if len(buffer) > MAX_LINE:
                session.send("400 BAD_REQUEST LINE_TOO_LONG")
                break

            # TCP เป็น byte stream ไม่ใช่ message ข้อมูลอาจมาติดกันหรือมาไม่ครบบรรทัด
            # จึงต้องตัดตาม TERMINATOR เอง แล้วเก็บเศษที่ยังไม่จบบรรทัดไว้ใน buffer
            while TERMINATOR in buffer:
                line, buffer = buffer.split(TERMINATOR, 1)
                line = line.strip()
                if not line:
                    continue

                print(f"[{addr}] Received: {line}")
                response, keep_alive = handle_line(session, line)

                if isinstance(response, list):
                    session.send(TERMINATOR.join(response))
                else:
                    session.send(response)

                if not keep_alive:
                    return
    except (ConnectionResetError, ConnectionAbortedError, OSError):
        pass
    finally:
        with SESSIONS_LOCK:
            if session in SESSIONS:
                SESSIONS.remove(session)
        conn.close()
        print(f"[DISCONNECTED] {addr} ตัดการเชื่อมต่อ")


def start_server():
    # เปิดห้องประมูลให้มีอายุ 1 ชั่วโมงนับจากตอน server เริ่มทำงาน
    opening = time.time() + 3600
    for auction in AUCTIONS.values():
        auction["closes_at"] = opening

    # สร้าง TCP Socket (AF_INET = IPv4, SOCK_STREAM = TCP)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()

    print(f"[STARTING] {PROTOCOL_ID} Server กำลังรันอยู่ที่ {HOST}:{PORT}")

    try:
        while True:
            conn, addr = server.accept()
            # แยก thread ให้แต่ละ client เพื่อให้คุยพร้อมกันได้หลายคน
            thread = threading.Thread(target=handle_client, args=(conn, addr),
                                      daemon=True)
            thread.start()
            print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")
    except KeyboardInterrupt:
        print("\n[STOPPING] ปิด server")
    finally:
        server.close()


if __name__ == "__main__":
    start_server()

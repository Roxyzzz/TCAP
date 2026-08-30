
import queue
import socket
import sys
import threading

HOST = "127.0.0.1"
PORT = 5555
ENCODING = "utf-8"
TERMINATOR = "\n"
BUFFER_SIZE = 1024
PROTOCOL_ID = "TCAP/1.0"


class LineReader:
    """อ่านข้อมูลจาก TCP stream แล้วตัดเป็นข้อความทีละบรรทัดตาม TERMINATOR"""

    def __init__(self, sock):
        self.sock = sock
        self.buffer = ""

    def read_line(self):
        while TERMINATOR not in self.buffer:
            chunk = self.sock.recv(BUFFER_SIZE)
            if not chunk:
                return None                      # server ปิดการเชื่อมต่อ
            self.buffer += chunk.decode(ENCODING, errors="replace")
        line, self.buffer = self.buffer.split(TERMINATOR, 1)
        return line.strip()


class TcapClient:
    def __init__(self, host=HOST, port=PORT, on_push=None):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.reader = LineReader(self.sock)
        self.responses = queue.Queue()
        self.closed = False
        # ค่าเริ่มต้น: พิมพ์ event ดิบตามเดิม (ใช้ในโหมด --demo / --raw)
        # โหมดเมนูจะส่ง callback ที่แปลเป็นข้อความอ่านง่ายเข้ามาแทน
        self.on_push = on_push or (lambda line: print(f"\n>>> [แจ้งเตือน] {line}"))
        # ต้องใช้ thread แยกอ่าน เพราะ server ส่ง event 9xx มาได้ตลอดเวลา
        # โดยที่ client ไม่ได้ส่งคำสั่งอะไรไป
        self.thread = threading.Thread(target=self._receive_loop, daemon=True)
        self.thread.start()

    def _receive_loop(self):
        try:
            while True:
                line = self.reader.read_line()
                if line is None:
                    break

                if line.startswith("9"):         # event ที่ server ส่งมาเอง
                    self.on_push(line)
                    continue

                # 202 AUCTION_LIST <n> / 205 CARD_LIST <n> ตามด้วยรายการอีก n บรรทัด
                # ต้องอ่านให้ครบทั้งก้อนก่อน ไม่งั้นบรรทัดรายการจะไปปนกับคำสั่งถัดไป
                if line.startswith("202 AUCTION_LIST") or line.startswith("205 CARD_LIST"):
                    count = int(line.split()[2])
                    block = [line]
                    for _ in range(count):
                        item = self.reader.read_line()
                        if item is None:
                            break
                        block.append(item)
                    self.responses.put(TERMINATOR.join(block))
                    continue

                self.responses.put(line)
        except OSError:
            pass
        finally:
            self.closed = True
            self.responses.put(None)

    def request(self, message):
        """ส่งหนึ่ง request แล้วรอ response (event 9xx ไม่นับ ถูกพิมพ์ไปแล้ว)"""
        self.sock.sendall((message + TERMINATOR).encode(ENCODING))
        try:
            return self.responses.get(timeout=5)
        except queue.Empty:
            return "(ไม่ได้รับการตอบกลับภายในเวลาที่กำหนด)"

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


# --------------------------------------------------------------------------
# แปลข้อความตอบกลับดิบของ TCAP ให้เป็นภาษาไทยอ่านง่าย (ใช้ในโหมดเมนู)
# --------------------------------------------------------------------------

def friendly_response(response):
    if response is None:
        return "(เซิร์ฟเวอร์ปิดการเชื่อมต่อ)"

    lines = response.split(TERMINATOR)
    head = lines[0].split()
    if not head:
        return response
    code = head[0]
    name = head[1] if len(head) > 1 else ""
    fields = head[2:]

    if code == "200":
        return f"✅ สำเร็จ: {' '.join(fields) or name}"
    if code == "201" and len(fields) >= 2:
        card_id, price, *name_parts = fields
        card_name = " ".join(name_parts) or "-"
        return f"💳 {card_name} ({card_id}) — ราคาตลาด {price} บาท"
    if code == "205":
        count = int(fields[0]) if fields else 0
        items = lines[1:1 + count]
        if count == 0:
            return "📋 ยังไม่มีการ์ดในฐานข้อมูล"
        out = [f"📋 การ์ดทั้งหมดในฐานข้อมูล ({count} ใบ):"]
        for item in items:
            parts = item.split(maxsplit=3)
            if len(parts) < 4:
                continue
            _, card_id, price, card_name = parts
            out.append(f"   • {card_id} — {card_name} : {price} บาท")
        return "\n".join(out)
    if code == "202":
        count = int(fields[0]) if fields else 0
        items = lines[1:1 + count]
        if count == 0:
            return "📋 ยังไม่มีห้องประมูลที่เปิดอยู่ในขณะนี้"
        out = [f"📋 ห้องประมูลที่เปิดอยู่ ({count} ห้อง):"]
        for item in items:
            parts = item.split()
            if len(parts) < 6:
                continue
            _, auc_id, card_id, bid, bidder, state = parts
            who = "ยังไม่มีผู้เสนอราคา" if bidder == "-" else f"ผู้นำอยู่ตอนนี้คือ {bidder}"
            out.append(f"   • {auc_id} | การ์ด {card_id} | ราคาล่าสุด {bid} บาท | {who} | สถานะ {state}")
        return "\n".join(out)
    if code == "203" and len(fields) >= 5:
        auc_id, card_id, bid, bidder, seconds_left = fields[:5]
        who = "ยังไม่มีผู้เสนอราคา" if bidder == "-" else f"ผู้นำอยู่ตอนนี้คือ {bidder}"
        minutes = int(seconds_left) // 60
        return (f"🏷️  ห้อง {auc_id} | การ์ด {card_id} | ราคาล่าสุด {bid} บาท\n"
                f"    {who} | เหลือเวลาอีกประมาณ {minutes} นาที")
    if code == "204" and len(fields) >= 2:
        auc_id, amount = fields[:2]
        return f"🎉 เสนอราคาสำเร็จ! คุณเสนอ {amount} บาท ในห้อง {auc_id}"
    if code == "210" and fields:
        return f"👀 เริ่มติดตามห้อง {fields[0]} แล้ว จะแจ้งเตือนทันทีที่มีคนเสนอราคาใหม่"
    if code == "211" and fields:
        return f"🔕 เลิกติดตามห้อง {fields[0]} แล้ว"
    if code == "299":
        return "👋 ออกจากระบบแล้ว ขอบคุณที่ใช้งาน"
    if code == "400":
        reason = " ".join(fields) or name
        return f"⚠️  คำขอไม่ถูกต้อง: {reason}"
    if code == "401":
        return "🔒 กรุณาเข้าสู่ระบบก่อนใช้งานคำสั่งนี้"
    if code == "404":
        target = fields[0] if fields else "รายการที่ค้นหา"
        return f"❌ ไม่พบข้อมูล: {target}"
    if code == "405":
        return f"❓ ไม่รู้จักคำสั่งนี้: {' '.join(fields) or name}"
    if code == "409" and fields:
        return f"⚠️  ราคาที่เสนอต่ำเกินไป ราคาปัจจุบันอยู่ที่ {fields[0]} บาท"
    if code == "410":
        return "⛔ ห้องประมูลนี้ปิดรับราคาแล้ว"
    if code == "426" and fields:
        return f"⚠️  เวอร์ชันโปรโตคอลไม่รองรับ: {fields[0]}"
    if code == "500":
        return "💥 เกิดข้อผิดพลาดที่ฝั่งเซิร์ฟเวอร์ ลองใหม่อีกครั้ง"
    if code == "900" and len(fields) >= 3:
        auc_id, amount, bidder = fields[:3]
        return f"\n🔔 [แจ้งเตือน] คุณโดนแซงราคาในห้อง {auc_id}! ราคาล่าสุด {amount} บาท โดย {bidder}"
    if code == "901" and len(fields) >= 3:
        auc_id, amount, bidder = fields[:3]
        return f"\n📢 [แจ้งเตือน] มีราคาใหม่ในห้อง {auc_id} ที่คุณติดตามอยู่: {amount} บาท โดย {bidder}"

    # เผื่อกรณีไม่รู้จักโค้ด: แสดงข้อความดิบไปก่อน
    return response


# --------------------------------- โหมดเมนู ---------------------------------

def print_banner():
    print("=" * 64)
    print("  TCAP Client — ระบบประมูลการ์ดเกมออนไลน์ (Trading Card Auction)")
    print("=" * 64)


def print_menu():
    print("""เลือกเมนู:
  1) ดูรายการการ์ด / เช็คราคาการ์ด
  2) ดูรายการห้องประมูลที่เปิดอยู่ทั้งหมด
  3) ดูรายละเอียดห้องประมูล
  4) เสนอราคาประมูล
  5) ติดตามห้องประมูล (รับแจ้งเตือนอัตโนมัติเมื่อมีคนเสนอราคาใหม่)
  6) เลิกติดตามห้องประมูล
  0) ออกจากโปรแกรม
""")


def ask(label, hint=None):
    prompt = f"{label} ({hint}): " if hint else f"{label}: "
    return input(prompt).strip()


def run_menu():
    print_banner()
    try:
        client = TcapClient(on_push=lambda line: print(friendly_response(line)))
    except ConnectionRefusedError:
        print("❌ เชื่อมต่อ Server ไม่ได้ กรุณาตรวจสอบว่าเปิด server.py ไว้อยู่หรือไม่")
        return

    hello = client.request(f"HELLO {PROTOCOL_ID}")
    if hello is None or not hello.startswith("200"):
        print(friendly_response(hello))
        client.close()
        return

    username = ""
    while not username:
        username = ask("ตั้งชื่อผู้ใช้เพื่อเข้าสู่ระบบ", "เช่น USER-01")
    print(friendly_response(client.request(f"LOGIN {username}")))
    print()

    try:
        while not client.closed:
            print_menu()
            choice = input("เลือกหมายเลขเมนู > ").strip()

            if choice in ("0", "q", "Q") or choice.upper() == "QUIT":
                print(friendly_response(client.request("QUIT")))
                break

            elif choice == "1":
                print(friendly_response(client.request("LIST_CARDS")))
                print()
                card_id = ask("พิมพ์รหัสการ์ดที่ต้องการดูราคา (เว้นว่างเพื่อข้าม)")
                if card_id:
                    print(friendly_response(client.request(f"GET_PRICE {card_id}")))

            elif choice == "2":
                print(friendly_response(client.request("LIST_AUCTIONS")))

            elif choice == "3":
                auc_id = ask("รหัสห้องประมูล", "เช่น AUC-001")
                print(friendly_response(client.request(f"GET_AUCTION {auc_id}")))

            elif choice == "4":
                auc_id = ask("รหัสห้องประมูลที่จะเสนอราคา", "เช่น AUC-001")
                amount = ask("จำนวนเงินที่จะเสนอ (บาท)")
                if not amount.isdigit():
                    print("⚠️  กรุณากรอกจำนวนเงินเป็นตัวเลขเท่านั้น ลองใหม่อีกครั้ง")
                    print()
                    continue
                print(friendly_response(client.request(f"PLACE_BID {auc_id} {amount}")))

            elif choice == "5":
                auc_id = ask("รหัสห้องประมูลที่จะติดตาม", "เช่น AUC-001")
                print(friendly_response(client.request(f"WATCH {auc_id}")))

            elif choice == "6":
                auc_id = ask("รหัสห้องประมูลที่จะเลิกติดตาม", "เช่น AUC-001")
                print(friendly_response(client.request(f"UNWATCH {auc_id}")))

            else:
                print("⚠️  กรุณาเลือกหมายเลขเมนูที่มีในรายการ (0-6)")
                print()
                continue

            print()
    except (EOFError, KeyboardInterrupt):
        print("\n👋 ปิดโปรแกรม")
    finally:
        client.close()


# ------------------------------ โหมดดิบ / สาธิต ------------------------------

RAW_HELP = """คำสั่งที่ใช้ได้ (พิมพ์ตามรูปแบบด้านล่าง แล้วกด Enter):
  HELLO TCAP/1.0
  LOGIN <user_id>
  LIST_CARDS                       ดูการ์ดทั้งหมดในฐานข้อมูลพร้อมราคา
  GET_PRICE <card_id>              เช่น GET_PRICE OP01-120
  LIST_AUCTIONS
  GET_AUCTION <auction_id>         เช่น GET_AUCTION AUC-001
  PLACE_BID <auction_id> <amount>  เช่น PLACE_BID AUC-001 46000
  WATCH <auction_id>
  UNWATCH <auction_id>
  QUIT
  HELP                             แสดงข้อความนี้อีกครั้ง
"""


def run_demo(client):
    """ทดสอบทุกคำสั่งของ TCAP ทั้งกรณีสำเร็จและกรณีผิดพลาด"""
    script = [
        f"HELLO {PROTOCOL_ID}",
        "PLACE_BID AUC-001 46000",       # ยังไม่ login -> 401
        "LOGIN USER-99",
        "LIST_CARDS",                     # -> 205 แบบหลายบรรทัด
        "GET_PRICE OP01-120",            # -> 201
        "GET_PRICE OP99-999",            # ไม่มีการ์ดนี้ -> 404
        "GET_PRICE",                     # ขาด argument -> 400
        "LIST_AUCTIONS",                 # -> 202 แบบหลายบรรทัด
        "GET_AUCTION AUC-001",           # -> 203
        "WATCH AUC-001",                 # -> 210
        "PLACE_BID AUC-001 46000",       # -> 204
        "PLACE_BID AUC-001 100",         # ต่ำกว่าราคาปัจจุบัน -> 409
        "PLACE_BID AUC-001 abc",         # จำนวนเงินไม่ถูกต้อง -> 400
        "PLACE_BID AUC-999 50000",       # ไม่มีห้องนี้ -> 404
        "BUY_NOW AUC-001",               # ไม่มีคำสั่งนี้ -> 405
        "QUIT",                          # -> 299
    ]

    for message in script:
        print(f"ส่ง : {message}")
        response = client.request(message)
        if response is None:
            print("รับ : (server ปิดการเชื่อมต่อ)\n")
            break
        indented = response.replace(TERMINATOR, TERMINATOR + "      ")
        print(f"รับ : {indented}\n")


def run_interactive(client, user_id):
    print(f"เชื่อมต่อ {PROTOCOL_ID} สำเร็จ (พิมพ์ HELP เพื่อดูคำสั่ง, QUIT เพื่อออก)\n")
    print(RAW_HELP)
    print(f"HELLO -> {client.request('HELLO ' + PROTOCOL_ID)}")
    if user_id:
        print(f"LOGIN -> {client.request('LOGIN ' + user_id)}")
    print()

    while not client.closed:
        try:
            message = input("TCAP> ").strip()
        except (EOFError, KeyboardInterrupt):
            message = "QUIT"
        if not message:
            continue
        if message.upper() in ("HELP", "?"):
            print(RAW_HELP)
            continue

        response = client.request(message)
        if response is None:
            print("(server ปิดการเชื่อมต่อ)")
            break
        print(response.replace(TERMINATOR, TERMINATOR + "      "))
        if message.upper().startswith("QUIT"):
            break


def _connect_or_explain():
    try:
        return TcapClient()
    except ConnectionRefusedError:
        print("ไม่สามารถเชื่อมต่อได้ กรุณาตรวจสอบว่า Server เปิดทำงานอยู่หรือไม่")
        return None


def main():
    args = sys.argv[1:]

    if "--raw" in args or "-i" in args:
        user_id = None
        if "-i" in args:
            index = args.index("-i")
            if index + 1 < len(args):
                user_id = args[index + 1]
        client = _connect_or_explain()
        if client is None:
            return
        try:
            run_interactive(client, user_id)
        finally:
            client.close()
        return

    if "--demo" in args:
        client = _connect_or_explain()
        if client is None:
            return
        try:
            run_demo(client)
        finally:
            client.close()
        return

    # ค่าเริ่มต้น: โหมดเมนู ใช้งานง่ายที่สุดสำหรับผู้ใช้ทั่วไป
    run_menu()


if __name__ == "__main__":
    main()

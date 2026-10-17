import threading
import queue
import socket
import os
from pathlib import Path
from dataclasses import dataclass
from src.model import NetworkManager

@dataclass
class FileInfo:
    name: str          
    relative_path: str  

class XenderController():
    def __init__(self, app, username):
        self.model = NetworkManager(self, username)
        self.app = app
        self.username: str = username
        self.running: bool = True
        self.is_connected = threading.Event()
        self.is_scanning = threading.Event()
        self.is_broadcasting = threading.Event()
        self.is_connecting = threading.Event()
    
        self.transfer_queue = queue.Queue()
        self.scanned_devices = None
        self.scanners = None

        self.transfer_worker_thread = threading.Thread(target=self._transfer_worker, daemon=True)
        self.transfer_worker_thread.start()

        self.receive_worker_thread = threading.Thread(target=self._receive_worker, daemon=True)
        self.receive_worker_thread.start()

    # <- SCAN ->

    def _scan_worker(self):
        devices: dict[str, tuple[str, int]] = {}
        print(f"Scanning... {len(devices)} found")

        print("Scanning for devices...\n")

        msg: bytes = b"I_SEE_U" + self.username.encode('utf-8')
        while self.is_scanning.is_set():
            try:
                name: str
                addr: tuple[str, int]
                name, addr = self.model.scan(msg, devices)
                if name:
                    devices[name] = addr
                    print(f"\nFound device: {name} ({addr[0]}:{addr[1]})")
                    print(f"Scanning... {len(devices)} found")
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[!]  Error scanning: {e}")
                break

            if devices and self.scanned_devices != devices:
                self.scanned_devices = devices
                print(f"Reporting scanned devices to window: {self.scanned_devices}")
                self.app.after(0, self.app.refresh_scan_devices, self.scanned_devices)

    def run_scan(self):
        self.model.init_scan_socks()
        
        self.is_scanning.set()
        scan_thread = threading.Thread(target=self._scan_worker, daemon=True)
        scan_thread.start()

    def end_scan(self):
        self.is_scanning.clear()
        print("Scanning stoppped")

    def scan_connect(self, addr):
        dev_ip: str = addr[0] # Ip address of device to intiate connnection with
        print("Addr value in sc_connect:", addr)
        while self.is_connecting.is_set():
            try:
                self.model.sc_connect(dev_ip)
                self.end_connecting()
                self.set_connection()
                # self.app.after(0, self.app.set_connected_user(next(k for k, v in self.scanned_devices if v == addr))) # Update the UI to show the connected user
                self.app.after(0, self.app.set_connected_user(dev_ip))
                print("Scanner connected!")
                break
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[!] Error connecting to: {e}.")
                self.end_connecting()
        
    # <- BROADCAST ->

    def _broadcast_worker(self):
        print("broadcasting...")
        dev: tuple[str, tuple[str, int]] = (None, None)

        username_bytes = self.username.encode('utf-8')
        message = bytes([len(username_bytes)]) + username_bytes + b"XENDER_DISCOVERY_REQUEST"

        while self.is_broadcasting.is_set():
            try:
                dev = self.model.broadcast(message)
            except socket.timeout:
                continue

            if dev != (None, None) and self.scanners !=  dev:
                self.scanners = dev
                self.app.after(0, self.app.refresh_scanners, self.scanners)

    def run_broadcast(self):
        self.model.init_bd_socks()

        self.is_broadcasting.set()
        broadcast_thread = threading.Thread(target=self._broadcast_worker, daemon=True)
        broadcast_thread.start()

        self.set_connecting()
        bd_conn_thread = threading.Thread(target=self.broadcast_connect, daemon=True)
        bd_conn_thread.start()

    def end_broadcast(self):
        self.is_broadcasting.clear()
        print("Broadcasting stopped")

    def broadcast_connect(self):
        print("Broacaster is connecting...")
        while self.is_connecting.is_set():
            try:
                self.model.bd_connect()
                self.end_connecting()
                self.set_connection()
                print("Broadcaster connected!")
                self.app.after(0, self.app.set_connected_user(self.scanners[0])) # Update the UI to show the connected user
                break

            except socket.timeout:
                continue
            except Exception as e:
                print(f"[!] Error connecting to: {e}.")
                self.end_connecting()

    def connect(self, dev_addr=None):
        self.set_connecting()
        # if mode == "scan":
        sc_conn_thread = threading.Thread(target=self.scan_connect, args=(dev_addr, ))
        sc_conn_thread.start()
        # elif mode == "broadcast":
        #     bd_conn_thread = threading.Thread(target=self.broadcast_connect)
        #     bd_conn_thread.start()

    def set_connecting(self):
        self.is_connecting.set()

    def end_connecting(self):
        self.is_connecting.clear()

    # <- SEND ->
    def set_connection(self):
        self.is_connected.set()

    def end_connection(self):
        self.is_connected.clear()

    def check_connection(self):
        return self.is_connected.is_set()

    def _transfer_worker(self):
        while self.running:
            if not self.is_connected.wait(timeout=1):
                continue

            try:
                transfer_kind, payload_path, progress_callback = self.transfer_queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                payload_path = Path(payload_path)
                if transfer_kind == "file":
                    name = os.path.basename(payload_path)
                    self.model._send_file(name, payload_path, progress_callback, rel_path=None)
                elif transfer_kind == "folder":
                    norm_folder_path = os.path.normpath(payload_path)
                    self.model.send_folder(norm_folder_path, progress_callback)
                else:
                    raise ValueError(f"Unknown transfer kind: {transfer_kind}")
            except Exception as e:
                print(f"Error sending {transfer_kind}: {e}")
            finally:
                self.transfer_queue.task_done()

    def send_file(self, path, progress_callback):
        self.transfer_queue.put(("file", path, progress_callback))

    def send_folder(self, path, progress_callback):
        self.transfer_queue.put(("folder", path, progress_callback))

    def register_incoming_transfer(self, item_name, total_bytes=None):
        return self.app.register_incoming_transfer(item_name, total_bytes)

    def _receive_worker(self):
        downloads_path: Path = Path.home() / "Downloads"
        while self.running:
            if not self.is_connected.wait(timeout=1):
                continue

            try:
                self.model.receive_transfer(downloads_path)

            except socket.timeout:
                continue
            
            except Exception as e:
                print(f"Error receiving transfer: {e}")

    def cancel_sender_transfer(self, sent_so_far: int = 0):
        self.model.sender_request_cancel(sent_so_far)

    def cancel_receiver_transfer(self, received_so_far: int = 0):
        self.model.receiver_request_cancel(received_so_far)

    def shut_down(self):
        print("Shutting down...")
        self.running = False
        self.end_scan()
        self.end_broadcast()
        self.end_connecting()
        self.end_connection()
        self.model.close_sockets()
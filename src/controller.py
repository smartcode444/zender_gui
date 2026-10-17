import threading
import queue
import socket
import os
from pathlib import Path
from dataclasses import dataclass
from src.model import NetworkManager
from src.window2 import P2PApp

@dataclass
class FileInfo:
    name: str          
    relative_path: str  

class XenderController():
    def __init__(self, app, username):
        self.model = NetworkManager(self, username)
        self.app: P2PApp = app
        self.username: str = username
        self.running: bool = True
        self.is_connected = threading.Event()
        self.is_scanning = threading.Event()
        self.is_broadcasting = threading.Event()
        self.is_connecting = threading.Event()
    
        self.send_file_queue = queue.Queue()
        self.send_folder_queue = queue.Queue()
        self.scanned_devices = None
        self.scanners = None

        self.send_file_worker_thread = threading.Thread(target=self._send_file_worker, daemon=True)
        self.send_file_worker_thread.start()

        self.send_folder_worker_thread = threading.Thread(target=self._send_folder_worker, daemon=True)
        self.send_folder_worker_thread.start()

        self.recieve_file_worker_thread = threading.Thread(target=self._recieve_file_worker, daemon=True)
        self.recieve_file_worker_thread.start()

        self.recieve_folder_worker_thread = threading.Thread(target=self._recieve_folder_worker, daemon=True)
        self.recieve_folder_worker_thread.start()

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

    def _send_file_worker(self):
        while self.running:
            if not self.is_connected.wait(timeout=1):
                continue

            try:
                file_path, progress_callback = self.send_file_queue.get(timeout=1)
            except queue.Empty:
                continue

            # Send the file over backend socket
            try:
                name = os.path.basename(file_path)
                self.model._send_file(name, file_path, progress_callback, rel_path=None)
            except Exception as e:
                print(f"Error sending file_path: {e}")
            finally:
                self.send_file_queue.task_done()

    def send_file(self, path, progress_callback):
        self.send_file_queue.put((path, progress_callback))

    def _send_folder_worker(self):
        while self.running:
            if not self.is_connected.wait(timeout=1):
                continue

            try:
                folder_path, progress_callback = self.send_folder_queue.get(timeout=1)
            except queue.Empty:
                continue

            # Send the file over backend socket
            try:
                norm_folder_path = os.path.normpath(folder_path)
                self.model.send_folder(norm_folder_path, progress_callback)
            except Exception as e:
                print(f"Error sending folder_path: {e}")
            finally:
                self.send_folder_queue.task_done()

    def send_folder(self, path, progress_callback):
        self.set_connection()
        self.send_folder_queue.put((path, progress_callback))

    def register_incoming_transfer(self, item_name):
        return self.app.register_incoming_transfer(item_name)

    def _recieve_file_worker(self):
        downloads_path: Path = Path.home() / "Downloads"
        while self.running:
            if not self.is_connected.wait(timeout=1):
                continue

            try:
                self.model.recieve_file(downloads_path)
            except Exception as e:
                print(f"Error sending folder_path: {e}")

    def _recieve_folder_worker(self):
        downloads_path: Path = Path.home() / "Downloads"
        while self.running:
            if not self.is_connected.wait(timeout=1):
                continue

            try:
                self.model.recieve_folder(downloads_path)
            except Exception as e:
                print(f"Error sending folder_path: {e}")

    def shut_down(self):
        print("Shutting down...")
        self.running = False
        self.end_scan()
        self.end_broadcast()
        self.end_connecting()
        self.end_connection()
        self.model.close_sockets()
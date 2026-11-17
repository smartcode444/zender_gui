import threading
import queue
import socket
import os
from pathlib import Path
from dataclasses import dataclass
from src.model import NetworkManager
import time
import re
from src.model import NetworkManager, build_discovery_response

@dataclass
class FileInfo:
    name: str          
    relative_path: str  

def log_time(func):
    def _log(*args, **kwargs):
        # 1. Clean up arguments string by stripping out messy hex memory addresses
        kwargs_str = f", {kwargs}" if kwargs else ""
        args_str = ", ".join(map(str, args))
        raw_call = f"{func.__name__}({args_str}{kwargs_str})"
        
        # Regex replaces "<class_name object at 0x...>" with "class_name()"
        clean_call = re.sub(r"<([^ ]+)\s+object\s+at\s+0x[0-9a-fA-F]+>", r"\1()", raw_call)
        
        # 2. Enforce a safe max length (e.g., 60 chars) so it never overflows the column
        MAX_COL_WIDTH = 60
        if len(clean_call) > MAX_COL_WIDTH:
            func_call = clean_call[:MAX_COL_WIDTH-3] + "..."
        else:
            func_call = clean_call
            
        time_start = time.perf_counter()
        result = func(*args, **kwargs)
        time_end = time.perf_counter()
        
        elapsed_time = f"{time_end - time_start:.6f}s"
        
        # 3. Print with matching column widths
        # Adjusting width from 45 to 60 to fit your network method names
        print(f"{func_call:<60} | {str(result):<15} | {elapsed_time:>10}")
        
        return result
    return _log

class XenderController():
    @log_time
    def __init__(self, app, username):
        self.model = NetworkManager(self, username)
        self.app = app
        self.username: str = username
        self.running: bool = True
        self.is_connected = threading.Event()
        self.is_scanning = threading.Event()
        self.is_broadcasting = threading.Event()
        self.is_connecting = threading.Event()
        self.transfer_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._background_threads: list[threading.Thread] = []
        self.scan_thread: threading.Thread | None = None
        self.broadcast_thread: threading.Thread | None = None

        self.transfer_queue = queue.Queue()
        self.scanned_devices = None
        self.scanners = None

    def run_transfer_thd(self):
        self.transfer_worker_thread = threading.Thread(target=self._transfer_worker)
        self.transfer_worker_thread.start()
        self._background_threads.append(self.transfer_worker_thread)

    def run_receive_thd(self):
        self.receive_worker_thread = threading.Thread(target=self._receive_worker)
        self.receive_worker_thread.start()
        self._background_threads.append(self.receive_worker_thread)


    # <- SCAN ->

    @log_time
    def _scan_worker(self):
        devices: dict[str, tuple[str, int]] = {}
        print(f"\nScanning for devices... {len(devices)} found\n")

        msg: bytes = build_discovery_response(self.username)
        while self.is_scanning.is_set():
            try:
                name: str
                addr: tuple[str, int]
                name, addr = self.model.scan(msg, devices)
                if name:
                    devices[name] = addr
                    print(f"\nFound device: {name} ({addr[0]}:{addr[1]})")
                    print(f"Scanning... {len(devices)} found\n")
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[!]  Error scanning: {e}")
                break

            if devices and self.scanned_devices != devices:
                self.scanned_devices = devices
                print(f"Reporting scanned devices to window: {self.scanned_devices}")
                self.app.after(0, self.app.refresh_scan_devices, self.scanned_devices)

    @log_time
    def run_scan(self):
        with self._state_lock:
            self.model.close_sockets()
            self.model.init_udp_socks()
            self.set_scanning()

        self.scan_thread = threading.Thread(target=self._scan_worker, daemon=True)
        self.scan_thread.start()
        self._background_threads.append(self.scan_thread)

    @log_time
    def end_scan(self):
        self.end_scanning()
        self.model.close_udp_socks()
        print("Scanning stoppped")

    @log_time
    def scan_connect(self, peer_name, addr):
        dev_ip: str = addr[0]  # Ip address of device to initiate connection with
        print("Addr value in sc_connect:", addr)
        while self.is_connecting.is_set():
            try:
                self.model.sc_connect(dev_ip)
                self.end_connecting()
                self.end_scan()
                self.set_connection()
                self.app.after(0, self.app.set_connected_user, peer_name)
                print("Scanner connected!")
                break
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[!] Error connecting to: {e}.")
                self.end_connecting()
                break

    @log_time
    def sc_connect(self, peer_name=None, dev_addr=None):
        if dev_addr is None and isinstance(peer_name, tuple):
            dev_addr = peer_name
            peer_name = None

        with self._state_lock:
            self.model.init_sc_socks()
            self.set_connecting()

        if dev_addr is None:
            print("No device address provided for connection.")
            self.end_connecting()
            return

        sc_conn_thread = threading.Thread(
            target=self.scan_connect,
            args=(peer_name or dev_addr[0], dev_addr),
            daemon=True,
        )
        sc_conn_thread.start()
        self._background_threads.append(sc_conn_thread)

    # <- BROADCAST ->

    @log_time
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
            except Exception as e:
                print(f"[!] Error broadcasting: {e}")
                break
            
            if dev != (None, None) and self.scanners !=  dev:
                self.scanners = dev
                self.app.after(0, self.app.refresh_scanners, self.scanners)

    @log_time
    def run_broadcast(self):
        with self._state_lock:
            self.model.close_sockets()
            self.model.init_udp_socks()
            self.set_broadcasting()

        self.broadcast_thread = threading.Thread(target=self._broadcast_worker, daemon=True)
        self.broadcast_thread.start()
        self._background_threads.append(self.broadcast_thread)

        with self._state_lock:
            self.model.init_bd_socks()
            self.set_connecting()

        bd_conn_thread = threading.Thread(target=self.broadcast_connect, daemon=True)
        bd_conn_thread.start()
        self._background_threads.append(bd_conn_thread)

    @log_time
    def end_broadcast(self): 
        self.end_broadcasting()
        self.model.close_bd_socks()
        self.model.close_udp_socks()
        print("Broadcasting stopped")

    @log_time
    def broadcast_connect(self):
        print("Broacaster is connecting...")
        while self.is_connecting.is_set():
            try:
                self.model.bd_connect()
                self.end_connecting()    # End the connecting state since the connection was successful
                self.end_broadcast()     # End the broadcasting state since the connection was successful
                self.set_connection()    # Connection established, set the connection state
                print("Broadcaster connected!")
                peer_name = self.scanners[0] if self.scanners else "Unknown peer"
                self.app.after(0, self.app.set_connected_user, peer_name)
                break

            except socket.timeout:
                continue
            except Exception as e:
                print(f"[!] Error connecting to: {e}.")
                self.end_connecting()

    @log_time
    def set_scanning(self):
        self.is_scanning.set()

    @log_time
    def end_scanning(self):
        self.is_scanning.clear()
    
    @log_time
    def set_broadcasting(self):
        self.is_broadcasting.set()

    @log_time
    def end_broadcasting(self): 
        self.is_broadcasting.clear()

    @log_time
    def set_connecting(self):
        self.is_connecting.set()

    @log_time
    def end_connecting(self):
        self.is_connecting.clear()

    @log_time
    def set_connection(self):
        self.is_connected.set()

    @log_time
    def end_connection(self):
        self.is_connected.clear()

    @log_time
    def check_connection(self):
        return self.is_connected.is_set()

    @log_time
    def _transfer_worker(self):
        while self.running:
            if not self.is_connected.wait(timeout=1):
                continue

            try:
                transfer_kind, payload_path, progress_callback = self.transfer_queue.get(timeout=1)
            except queue.Empty:
                continue

            if not self.running:
                self.transfer_queue.task_done()
                break

            with self.transfer_lock:
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
                    if self.running:
                        print(f"Error sending {transfer_kind}: {e}")
                finally:
                    self.transfer_queue.task_done()

    @log_time
    def send_file(self, path, progress_callback):
        self.transfer_queue.put(("file", path, progress_callback))

    @log_time
    def send_folder(self, path, progress_callback):
        self.transfer_queue.put(("folder", path, progress_callback))

    @log_time
    def register_incoming_transfer(self, item_name, total_bytes=None):
        return self.app.register_incoming_transfer(item_name, total_bytes)

    @log_time
    def _receive_worker(self):
        downloads_path: Path = Path.home() / "Downloads" / "Zender"
        downloads_path.mkdir(parents=True, exist_ok=True)
        while self.running:
            if not self.is_connected.wait(timeout=1):
                continue

            with self.transfer_lock:
                try:
                    self.model.receive_transfer(downloads_path)

                except socket.timeout:
                    continue
                
                except Exception as e:
                    if self.running:
                        print(f"Error receiving transfer: {e}")

    @log_time
    def cancel_sender_transfer(self, sent_so_far: int = 0):
        self.model.sender_request_cancel(sent_so_far)

    @log_time
    def cancel_receiver_transfer(self, received_so_far: int = 0):
        self.model.receiver_request_cancel(received_so_far)

    @log_time
    def shut_down(self):
        print("Shutting down...")
        self.running = False
        self.end_connecting()
        self.end_connection()
        self.end_scan()
        self.end_broadcast()
        self.model.close_sockets()

        for thread in self._background_threads:
            if thread.is_alive():
                thread.join(timeout=2.0)
import threading
import queue
import socket
import os
from pathlib import Path
from dataclasses import dataclass
from tkinter import filedialog
from src.model import NetworkManager
# from model import NetworkManager

@dataclass
class FileInfo:
    name: str          
    relative_path: str  

def async_key_pressed():
    pass

class XenderController():
    def __init__(self, app, username):
        self.model = NetworkManager(self, username)
        self.app = app
        self.username = username
        self.running = True
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
        devices = {}
        print(f"Scanning... {len(devices)} found")
        self.model.init_scan_socks()

        print("Scanning for devices...\n")

        msg = b"I_SEE_U" + self.username.encode('utf-8')
        while self.is_scanning.is_set():
            try:
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
        self.is_scanning.set()
        scan_thread = threading.Thread(target=self._scan_worker, daemon=True)
        scan_thread.start()

    def end_scan(self):
        self.is_scanning.clear()
        print("Scanning stoppped")

    def scan_connect(self, addr):
        dev_ip = addr[0] # Ip address of device to intiate connnection with
        print("Addr value in sc_connect:", addr)
        while self.is_connecting:
            try:
                self.model.sc_connect(dev_ip)
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[!] Error connecting to: {e}.")
                break
        
    # <- BROADCAST ->

    def _broadcast_worker(self):
        print("broadcasting...")
        self.model.init_bd_socks()

        username_bytes = self.username.encode('utf-8')
        message = bytes([len(username_bytes)]) + username_bytes + b"XENDER_DISCOVERY_REQUEST"

        while self.is_broadcasting.is_set():
            try:
                dev = self.model.broadcast(message)
                # if dev:
                #     # print(dev)
                #     pass
            except socket.timeout:
                continue

            if dev and self.scanners !=  dev:
                self.scanners = dev
                self.app.after(0, self.app.refresh_scanners, self.scanners)

    def run_broadcast(self):
        self.is_broadcasting.set()
        broadcast_thread = threading.Thread(target=self._broadcast_worker, daemon=True)
        broadcast_thread.start()

    def end_broadcast(self):
        self.is_broadcasting.clear()
        print("Broadcasting stopped")


    def broadcast_connect(self):
        while self.is_connecting:
            try:
                self.model.bd_connect()
                break
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[!] Error connecting to: {e}.")
                break

    def connect(self, mode, dev_addr=None):
        self.is_connecting.set()
        if mode == "scan":
            sc_conn_thread = threading.Thread(target=self.scan_connect, args=(dev_addr, ))
            sc_conn_thread.start()
        elif mode == "broadcast":
            bd_conn_thread = threading.Thread(target=self.broadcast_connect)
            bd_conn_thread.start()

    def end_connecting(self):
        self.is_connecting.clear()

    # <- SEND ->
    def set_connection(self):
        self.is_connected.set()

    def end_connection(self):
        self.is_connected.clear()

    def _send_file_worker(self):
        while self.is_connected.is_set():
            try:
                file_path, progress_callback = self.send_file_queue.get(timeout=1)
            except queue.Empty:
                continue

            # Send the file over backend socket
            try:
                path = file_path.name
                name = os.path.basename(path)
                self.model._send_file(name, path, progress_callback, rel_path=None)
            except Exception as e:
                print(f"Error sending file_path: {e}")
            finally:
                self.send_file_queue.task_done()

    def send_file(self, path, progress_callback):
        self.set_connection()
        self.send_folder_queue.put((path, progress_callback))

    def _send_folder_worker(self):
        while self.is_connected.is_set():
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
        self.send_queue.put((path, progress_callback))

    def register_incoming_transfer(self, item_name):
        return self.app.register_incoming_transfer(item_name)

    def _recieve_file_worker(self):
        downloads_path = Path.home() / "Downloads"
        while self.is_connected.is_set():
            try:
                self.model.recieve_file(downloads_path)
            except Exception as e:
                print(f"Error sending folder_path: {e}")

    def _recieve_folder_worker(self):
        downloads_path = Path.home() / "Downloads"
        while self.is_connected.is_set():
            try:
                self.model.recieve_folder(downloads_path)
            except Exception as e:
                print(f"Error sending folder_path: {e}")
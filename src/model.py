import socket
import select
import threading
import struct
import psutil
import os
from pathlib import Path
from enum import IntEnum
import time
import re

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

class TransferStatus(IntEnum):
    SUCCESS = 0
    LOADING = 1
    ERROR = 2
    CANCELLED = 3


DISCOVERY_REQUEST_TAG = "XENDER_DISCOVERY_REQUEST"
DISCOVERY_RESPONSE_PREFIX = b"I_SEE_U"
CANCEL_REQUEST_ID = 0x03
FILE_TRANSFER_TAG = b"F"
FOLDER_TRANSFER_TAG = b"D"


def build_discovery_request(username: str) -> bytes:
    username_bytes = username.encode("utf-8")
    return bytes([len(username_bytes)]) + username_bytes + DISCOVERY_REQUEST_TAG.encode("utf-8")


def build_discovery_response(username: str) -> bytes:
    return DISCOVERY_RESPONSE_PREFIX + username.encode("utf-8")


def decode_discovery_packet(data: bytes):
    if not data:
        return None, None

    username_len = data[0]
    if not 0 < username_len <= len(data) - 1:
        return None, None

    username = data[1:1 + username_len].decode("utf-8", errors="strict")
    payload = data[1 + username_len:]
    payload_text = payload.decode("utf-8", errors="strict")
    return username, payload_text


def encode_cancel_packet(bytes_transferred: int) -> bytes:
    return bytes([CANCEL_REQUEST_ID]) + int(bytes_transferred).to_bytes(8, "big")


def decode_cancel_packet(packet: bytes) -> int:
    if len(packet) != 9 or packet[0] != CANCEL_REQUEST_ID:
        raise ValueError("Invalid cancel packet")
    return int.from_bytes(packet[1:], "big")


def encode_file_header(name: str, file_size: int) -> bytes:
    name_bytes = name.encode("utf-8")
    return len(name_bytes).to_bytes(4, "big") + name_bytes + file_size.to_bytes(8, "big")


def encode_folder_header(folder_name: str, folder_size: int, file_count: int) -> bytes:
    folder_name_bytes = folder_name.encode("utf-8")
    return (
        folder_size.to_bytes(8, "big")
        + len(folder_name_bytes).to_bytes(4, "big")
        + folder_name_bytes
        + file_count.to_bytes(4, "big")
    )


def resolve_unique_path(path: str | os.PathLike[str]) -> Path:
    """Return the original path or a numbered collision-safe alternate."""
    candidate = Path(path)
    if not candidate.exists():
        return candidate

    stem = candidate.stem or candidate.name
    suffix = candidate.suffix
    counter = 1
    while True:
        next_name = f"{stem} ({counter}){suffix}" if suffix else f"{stem} ({counter})"
        next_candidate = candidate.with_name(next_name)
        if not next_candidate.exists():
            return next_candidate
        counter += 1


class NetworkManager:
    @log_time
    def __init__(self, controller, username: str):
        self.controller        = controller

        self.UDP_PORT          = 40017
        self.TCP_PORT          = 40018
        self.CTRL_PORT         = 40019
        self.CONNECTION_TIMEOUT = 10.0
        self.INTERFACES        = get_all_broadcast_addresses()
        self.own_ips           = {ip for _, ip in self.INTERFACES}

        self.DISCOVERY_MESSAGE = "XENDER_DISCOVERY_REQUEST"
        self.RESPONSE_MESSAGE  = "I_SEE_U"

        self.name              = username

        self.tcp_server        = None
        self.ctrl_server       = None
        self.tcp_conn          = None 
        self.ctrl_conn         = None

        self._cancel_receiving_flag    = threading.Event()
        self._cancel_sending_flag      = threading.Event()

        self.selected_mode     = None

    @log_time
    def init_udp_socks(self):
        if getattr(self, "udp_socket", None) is not None:
            try:
                self.udp_socket.close()
            except OSError:
                pass
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 0)
        self.udp_socket.bind(('', self.UDP_PORT))
        self.udp_socket.settimeout(1.0)

    @log_time
    def close_udp_socks(self):
        sock = getattr(self, "udp_socket", None)
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
            finally:
                setattr(self, "udp_socket", None)

    def _get_udp_socket(self):
        sock = getattr(self, "udp_socket", None)
        if sock is None:
            return None
        try:
            sock.fileno()
        except OSError:
            setattr(self, "udp_socket", None)
            return None
        return sock

    @log_time   
    def close_sockets(self):
        """Close all sockets."""
        for name in (
            "tcp_conn",
            "ctrl_conn",
            "tcp_server",
            "ctrl_server",
            "udp_socket",
        ):
            sock = getattr(self, name, None)
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
                finally:
                    setattr(self, name, None)
    
    @log_time
    def init_bd_socks(self):
        """Intialize tcp sockets for broadcaster."""
        self.selected_mode = "broadcast"

        self.ctrl_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.ctrl_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
        self.ctrl_server.bind(('', self.CTRL_PORT))
        self.ctrl_server.listen(1)

        self.tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
        self.tcp_server.bind(('', self.TCP_PORT))
        self.tcp_server.listen(1)

    @log_time
    def init_sc_socks(self):
        """Intialize tcp sockets for scanner."""
        self.selected_mode = "scan"
        self.tcp_conn  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)        
        self.ctrl_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

    @log_time
    def close_bd_socks(self):
        """Close broadcast sockets."""
        for name in ("tcp_server", "ctrl_server"):
            sock = getattr(self, name, None)
            if sock is not None:
                sock.close()
                setattr(self, name, None)

    @log_time
    def close_transfer_socks(self):
        """Close transfer sockets."""
        for name in ("tcp_conn", "ctrl_conn"):
            sock = getattr(self, name, None)
            if sock is not None:
                sock.close()
                setattr(self, name, None)

    # @log_time
    def broadcast(self, message) -> str:
        """Send discovery packet on every interface, and wait for a response"""
        sock = self._get_udp_socket()
        if sock is None:
            return None, None

        for brd_addr, _ in self.INTERFACES:
            try:
                sock.sendto(message, (brd_addr , self.UDP_PORT))
            except OSError:
                pass # Interface may be down or unreachable

        try:
            data, addr = sock.recvfrom(1024)
            if not data:
                return None, None
        except socket.timeout:
            raise
        except OSError:
            return None, None

        if data.startswith(message):
            return None, None

        decoded = data.decode('utf-8', errors='ignore').strip()
        if decoded.startswith('I_SEE_U'):
            device_name = decoded[7:]
            return device_name, addr

        return None, None

    @log_time
    def bd_connect(self):
        """Accept all three channels for one incoming connection attempt."""
        listeners = (
            self.tcp_server,
            self.ctrl_server,
        )
        for listener in listeners:
            listener.settimeout(self.CONNECTION_TIMEOUT)

        try:
            self.tcp_conn,  _ = self.tcp_server.accept()
            self.ctrl_conn, _ = self.ctrl_server.accept()

        except socket.timeout:
            raise socket.timeout 
    
        except Exception as e:
            raise e

    # @log_time
    def scan(self, msg, devices) -> tuple:
        """Scan for devices on the network."""
        sock = self._get_udp_socket()
        if sock is None:
            return None, None

        try:
            data, address = sock.recvfrom(1024)
            if not data:
                return None, None
        except socket.timeout:
            raise socket.timeout
        except OSError:
            return None, None

        sender_ip = address[0]
        if sender_ip in self.own_ips:
            return None, None

        client_name, client_msg = decode_discovery_packet(data)
        if client_name is None:
            return None, None

        if client_msg == DISCOVERY_REQUEST_TAG and client_name not in devices:
            try:
                sock.sendto(msg, address)
            except OSError:
                return None, None

            return client_name, address

        return None, None

    @log_time
    def sc_connect(self, device_addr):
        print("Device address in sc_connect:", device_addr)
        try:
            for connection in (self.tcp_conn, self.ctrl_conn):
                connection.settimeout(self.CONNECTION_TIMEOUT)

            self.tcp_conn.connect((device_addr, self.TCP_PORT))
            self.ctrl_conn.connect((device_addr, self.CTRL_PORT))

        except socket.timeout:
            raise socket.timeout

        except Exception as e:
            raise e

    @log_time
    def set_cancel_recv_flag(self):
        self._cancel_receiving_flag.set()

    @log_time
    def clear_cancel_recv_flag(self):
        self._cancel_receiving_flag.clear()

    @log_time
    def set_cancel_send_flag(self):
        self._cancel_sending_flag.set()

    @log_time
    def clear_cancel_send_flag(self):
        self._cancel_sending_flag.clear()

    # <- SEND ->

    # Sender side - on cancel, report exactly how many bytes it pushed into the socket
    def _send_exact(self, sock, payload: bytes):
        view = memoryview(payload)
        while view:
            sent = sock.send(view)
            if sent == 0:
                raise ConnectionError("Socket closed while writing payload")
            view = view[sent:]

    def _recv_exact(self, sock, size: int, timeout: float | None = None):
        if timeout is not None:
            sock.settimeout(timeout)

        chunks = []
        remaining = size
        while remaining > 0:
            try:
                chunk = sock.recv(remaining)
            except socket.timeout:
                raise
            except OSError:
                raise

            if not chunk:
                raise ConnectionError("Socket closed while reading exact-length payload")

            chunks.append(chunk)
            remaining -= len(chunk)

        return b"".join(chunks)

    @log_time
    def sender_notify_cancel(self, local_sent_so_far: int):
        """Send a CANCEL byte (0x03) and the amount of bytes pushed to the socket."""
        self.clear_cancel_send_flag()
        try:
            self.ctrl_conn.sendall(encode_cancel_packet(local_sent_so_far))
        except OSError:
            pass

    @log_time
    def sender_handle_cancel(self, local_sent_so_far: int):
        self.clear_cancel_send_flag()
        try:
            cmd = self._recv_exact(self.ctrl_conn, 1, timeout=1.0)
            if cmd != b'\x03':
                return

            peer_received = decode_cancel_packet(self._recv_exact(self.ctrl_conn, 9, timeout=1.0))
            gap = max(0, local_sent_so_far - peer_received)
            discarded = 0
            while discarded < gap:
                chunk = self.tcp_conn.recv(min(65536, gap - discarded))
                if not chunk:
                    break
                discarded += len(chunk)
        except (ConnectionError, OSError, socket.timeout, ValueError):
            return

    @log_time
    def receiver_notify_cancel(self, local_received_so_far: int):
        """Send a CANCEL packet to the sender with how many bytes were received."""
        self.clear_cancel_recv_flag()
        if self.ctrl_conn is None:
            return

        try:
            self.ctrl_conn.sendall(encode_cancel_packet(local_received_so_far))
        except OSError:
            pass

    # Receiver side - Receives cancel signal 
    @log_time
    def receiver_handle_cancel(self, local_receiver_received, timeout: float = 1.0):
        """Drain exactly remaining bytes from the data socket and discard."""
        self.clear_cancel_recv_flag()
        try:
            self.ctrl_conn.settimeout(timeout)
        except OSError:
            return

        try:
            cmd = self._recv_exact(self.ctrl_conn, 1, timeout=timeout)
            if cmd == b'\x03':
                peer_sender_sent = decode_cancel_packet(self._recv_exact(self.ctrl_conn, 9, timeout=timeout))
                gap = max(0, peer_sender_sent - local_receiver_received)
                discarded = 0
                while discarded < gap:
                    chunk = self.tcp_conn.recv(min(65536, gap - discarded))
                    if not chunk:
                        break
                    discarded += len(chunk)
            self.ctrl_conn.settimeout(None)
        except (ConnectionError, OSError, socket.timeout, ValueError):
            try:
                self.ctrl_conn.settimeout(None)
            except Exception:
                pass
            return
    
    @log_time
    def sender_request_cancel(self, local_sent_so_far: int):
        """Public helper: mark this transfer as cancelled and notify the receiver."""
        self.set_cancel_send_flag()
        self.sender_notify_cancel(local_sent_so_far)

    @log_time
    def receiver_request_cancel(self, local_received_so_far: int):
        """Public helper: mark this transfer as cancelled and notify the receiver."""
        self.set_cancel_recv_flag()
        self.receiver_notify_cancel(local_received_so_far)

    @log_time
    def _send_file(self, name, path, progress_callback, rel_path: str = None):
        """Send a single file over the data socket.
        Arguments:
          - name: base filename shown to receiver
          - path: local filesystem path to read
          - progress_callback(progress: float, status: TransferStatus)
          - rel_path: optional relative path to send instead of `name` (for folders)

        The header layout matches the receiver expected layout:
          [4B name_len][name_bytes][8B file_size]

        This method starts a small watcher thread that listens on the control
        channel for a CANCEL byte (0x03). If the local user cancels the transfer
        `sender_notify_cancel()` should be called which will cause this function to
        send a CANCEL byte to the peer and abort the transfer.
        """
        # Use the provided relative path name for header if given
        self.ctrl_conn.settimeout(None)
        self.tcp_conn.settimeout(None)

        header_name = rel_path if rel_path is not None else name

        self.clear_cancel_send_flag()
        status = TransferStatus.LOADING

        filesize = os.path.getsize(path)
        header = encode_file_header(header_name, filesize)

        try:
            self._send_exact(self.tcp_conn, FILE_TRANSFER_TAG)
            self._send_exact(self.tcp_conn, header)

            sent = 0
            last_reported = 0
            with open(path, 'rb') as f:
                while sent < filesize:
                    readable, _, _ = select.select([self.ctrl_conn], [], [], 0.1)
                    # If receiver cancel transfer, handle the cancel signal
                    if readable:
                        self.sender_handle_cancel(sent)
                        progress_callback(sent / max(1, filesize), status)
                        return

                    if self._cancel_sending_flag.is_set():
                        # local or remote requested cancel -> notify peer and abort
                        try:
                            self.sender_notify_cancel(sent)
                            return
                        except Exception:
                            pass
                        status = TransferStatus.ERROR
                        progress_callback(sent / max(1, filesize), status)
                        return

                    # Send next chunk
                    chunk = f.read(65536)
                    if not chunk:
                        break

                    try:
                        self._send_exact(self.tcp_conn, chunk)
                    except (OSError, BlockingIOError) as e:
                        print("Network error - connection lost")
                        status = TransferStatus.ERROR
                        if progress_callback is not None:
                            progress_callback(sent / max(1, filesize), status)
                        try:
                            self.sender_notify_cancel(sent)
                            return
                        except Exception:
                            pass
                        raise RuntimeError(f"[!] Error while sending '{name}': {e}")

                    sent += len(chunk)
                    if progress_callback is not None and (sent - last_reported >= 65536 or sent == filesize):
                        progress_callback(sent / max(1, filesize), status)
                        last_reported = sent

            status = TransferStatus.SUCCESS
            if progress_callback is not None:
                progress_callback(1.0, status)

        finally:
            try:
                # ensure watcher thread will exit soon
                self.set_cancel_send_flag()
            except Exception as e:
                pass

    @log_time
    def _send_folder(self, folder_path, progress_callback):
        self.ctrl_conn.settimeout(None)
        self.tcp_conn.settimeout(None)

        # Build the list of files and total size for progress calculation
        folder_size = 0
        files_list = []  # list of tuples (full_path, rel_path)

        base_foldername = os.path.basename(folder_path)
        for root_dir, _, files in os.walk(folder_path):
            for file in files:
                full_path = os.path.join(root_dir, file)
                rel_path = os.path.relpath(full_path, folder_path)
                rel_path = os.path.join(base_foldername, rel_path).replace(os.sep, '/')
                files_list.append((full_path, rel_path))
                folder_size += os.path.getsize(full_path)

        # Mark this transfer as a folder, then send folder metadata to receiver
        self._send_exact(self.tcp_conn, FOLDER_TRANSFER_TAG)
        self._send_exact(self.tcp_conn, encode_folder_header(base_foldername, folder_size, len(files_list)))

        total_sent = 0
        for full_path, rel_path in files_list:
            if self._cancel_sending_flag.is_set():
                raise RuntimeError(f"[!] Transfer cancelled before sending '{rel_path}'")

            try:
                # Use the same internal _send_file but pass rel_path so receiver can reconstruct
                def _file_progress(pct, status):
                    # Map file-level progress into folder-level progress
                    nonlocal total_sent
                    # pct is file-level fraction; compute absolute bytes sent approximation
                    bytes_sent = int(pct * os.path.getsize(full_path))
                    # report overall progress as bytes_sent / folder_size
                    progress_callback(min(1.0, (total_sent + bytes_sent) / max(1, folder_size)), status)

                self._send_file(os.path.basename(full_path), full_path, _file_progress, rel_path=rel_path)
                total_sent += os.path.getsize(full_path)

            except Exception as e:
                # bubble up error so caller can mark failed and update UI
                raise

        # finished sending entire folder
        return True

    @log_time
    def send_folder(self, folder_path, progress_callback):
        return self._send_folder(folder_path, progress_callback)

    @log_time
    def send_file(self, path, name=None, progress_callback=None):
        if name is None:
            name = os.path.basename(path)
        return self._send_file(name, path, progress_callback)

    @log_time
    def _receive_file(self, dest_folder):
        """Receive files over the data socket. 
        Shuts down socket on cancellation so sender unblocks."""
        self.ctrl_conn.settimeout(None)
        self.tcp_conn.settimeout(None)

        self.clear_cancel_recv_flag()
        status = TransferStatus.LOADING

        def recv_exact(n: int) -> bytes:
            """Await exactly n bytes"""
            buf = b''
            while len(buf) < n:
                readable, _, _ = select.select([self.tcp_conn, self.ctrl_conn], [], [], 5.0)

                if not readable:
                    raise socket.timeout

                if self.ctrl_conn in readable:
                    self.receiver_handle_cancel(len(buf))
                    raise RuntimeError("Sender cancelled - Stop recieving")

                # User click cancel button
                if self._cancel_receiving_flag.is_set():
                    self.receiver_handle_cancel(len(buf))
                    raise RuntimeError("[!] Transfer cancelled by sender")

                if self.tcp_conn in readable:
                    chunk = self.tcp_conn.recv(n - len(buf))
                    if not chunk:
                        raise ConnectionError("[!] Connection closed by sender")
                    buf += chunk
            return buf

        try:
            received = 0
            name_len = int.from_bytes(recv_exact(4), 'big')
            filename = (recv_exact(name_len)).decode('utf-8')

            filesize = int.from_bytes(recv_exact(8), 'big')
            progress_callback = self.controller.register_incoming_transfer(filename, filesize)

            # Receive file
            filepath = str(resolve_unique_path(os.path.join(dest_folder, filename)))
            dest_dir = os.path.dirname(filepath)
            if dest_dir:
                os.makedirs(dest_dir, exist_ok=True)
            with open(filepath, "wb") as f:
                last_reported = 0
                while received < filesize:
                    # Watch Both sockets simultaneously 
                    readable, _, _ = select.select([self.tcp_conn, self.ctrl_conn], [], [], 5.0)

                    if not readable:
                        raise socket.timeout

                    if self.ctrl_conn in readable:
                        self.receiver_handle_cancel(received)
                        raise RuntimeError("Sender cancelled - Stop recieving")

                    if self._cancel_receiving_flag.is_set():
                        self.receiver_handle_cancel(received)
                        raise RuntimeError("[!] Transfer cancelled by sender")

                    if self.tcp_conn in readable:
                        chunk = self.tcp_conn.recv(
                            min(65536, filesize - received)
                        )
                        if not chunk:
                            raise ConnectionError("\n[!] Connection lost mid-transfer")
                        f.write(chunk)
                        received += len(chunk)
                        if received - last_reported >= 65536 or received == filesize:
                            progress_callback(received / max(1, filesize), status)
                            last_reported = received

            status = TransferStatus.SUCCESS
            progress_callback(received / max(1, filesize), status)

        except (ConnectionError, BlockingIOError, RuntimeError) as e:
            status = TransferStatus.ERROR
            # best-effort progress update
            try:
                progress_callback(received / max(1, filesize), status)
            except Exception:
                pass
            raise

    @log_time    
    def _receive_folder(self, dest_folder):
        """Receive folders"""
        self.ctrl_conn.settimeout(None)
        self.tcp_conn.settimeout(None)

        self.clear_cancel_recv_flag()
        status = TransferStatus.LOADING

        def recv_exact(n: int) -> bytes:
            """Await exactly n bytes"""
            buf = b''
            while len(buf) < n:
                readable, _, _ = select.select([self.tcp_conn, self.ctrl_conn], [], [], 5.0)
                
                if not readable:
                    raise socket.timeout

                if self.ctrl_conn in readable:
                    self.receiver_handle_cancel(len(buf))
                    raise RuntimeError("Sender cancelled - Stop recieving")

                if self._cancel_receiving_flag.is_set():
                    self.receiver_handle_cancel(len(buf))
                    raise RuntimeError("[!] Transfer cancelled by sender")

                if self.tcp_conn in readable:
                    chunk = self.tcp_conn.recv(n - len(buf))
                    if not chunk:
                        raise ConnectionError("[!] Connection closed by sender")
                    buf += chunk
            return buf

        folder_size = int.from_bytes(recv_exact(8), 'big')
        folder_name_len = int.from_bytes(recv_exact(4), 'big')
        folder_name = recv_exact(folder_name_len).decode('utf-8')
        file_count = int.from_bytes(recv_exact(4), 'big')

        received_folder = 0
        remaining_files = file_count
        while remaining_files > 0:
            try:
                path_name_len = int.from_bytes(recv_exact(4), 'big')
                relpath = (recv_exact(path_name_len)).decode('utf-8')

                filesize = int.from_bytes(recv_exact(8), 'big')
                folder_entry_name = os.path.basename(relpath)
                progress_callback = self.controller.register_incoming_transfer(folder_entry_name, filesize)

                # Receive file
                filepath = str(resolve_unique_path(os.path.join(dest_folder, relpath)))
                dest_dir = os.path.dirname(filepath)
                if dest_dir:
                    os.makedirs(dest_dir, exist_ok=True)  # Recreate directory structure in receiver end

                with open(filepath, "wb") as f:
                    received_file = 0
                    last_reported = 0

                    while received_file < filesize:
                        # Watch Both sockets simultaneously 
                        readable, _, _ = select.select([self.tcp_conn, self.ctrl_conn], [], [], 5.0)

                        if not readable:
                            raise socket.timeout

                        if self.ctrl_conn in readable:
                            self.receiver_handle_cancel(received_file)
                            raise RuntimeError("Sender cancelled - Stop recieving")

                        if self._cancel_receiving_flag.is_set():
                            self.receiver_handle_cancel(received_file)
                            raise RuntimeError("[!] Transfer cancelled by sender")

                        if self.tcp_conn in readable:
                            chunk = self.tcp_conn.recv(
                                min(65536, filesize - received_file)
                            )
                            if not chunk:
                                raise ConnectionError("\n[!] Connection lost mid-transfer")
                            f.write(chunk)
                            received_file += len(chunk)
                            received_folder += len(chunk)
                            if received_file - last_reported >= 1048576 or received_file == filesize:
                                progress_callback(received_folder / max(1, folder_size), status)
                                last_reported = received_file

                remaining_files -= 1
                status = TransferStatus.SUCCESS
                progress_callback(received_folder / max(1, folder_size), status)

            except (ConnectionError, BlockingIOError, RuntimeError) as e:
                status = TransferStatus.ERROR
                # best-effort progress update
                try:
                    progress_callback(received_folder / max(1, folder_size), status)
                except Exception:
                    pass
                raise

    @log_time
    def receive_folder(self, dest_folder):
        return self._receive_folder(dest_folder)
    
    @log_time
    def receive_file(self, dest_folder):
        return self._receive_file(dest_folder)

    @log_time
    def receive_transfer(self, dest_folder):
        """Read the next transfer type and dispatch to the matching receiver."""
        def recv_exact(n: int) -> bytes:
            buf = b''
            while len(buf) < n:
                readable, _, _ = select.select([self.tcp_conn, self.ctrl_conn], [], [], 5.0)

                if not readable:
                    self.receiver_handle_cancel(len(buf))
                    raise RuntimeError("Timeout - connection may be dead")

                if self.ctrl_conn in readable:
                    self.receiver_handle_cancel(len(buf))
                    raise RuntimeError("Sender cancelled - Stop recieving")

                if self._cancel_receiving_flag.is_set():
                    self.receiver_handle_cancel(len(buf))
                    raise RuntimeError("[!] Transfer cancelled by sender")

                if self.tcp_conn in readable:
                    chunk = self.tcp_conn.recv(n - len(buf))
                    if not chunk:
                        raise ConnectionError("[!] Connection closed by sender")
                    buf += chunk
            return buf

        transfer_kind = recv_exact(1)
        if transfer_kind == FILE_TRANSFER_TAG:
            return self._receive_file(dest_folder)
        if transfer_kind == FOLDER_TRANSFER_TAG:
            return self._receive_folder(dest_folder)

        raise RuntimeError(f"Unknown transfer type: {transfer_kind!r}")

@log_time
def get_all_broadcast_addresses():
    """
    Returns a list of (broadcast_address, own_ip) for every
    active non-loopback network interface on this machine.
    """
    results = []
    seen    = set()

    for iface_name, addrs in psutil.net_if_addrs().items():
        for addr in addrs:

            # We only care about IPv4 addresses
            if addr.family != socket.AF_INET:
                continue

            ip      = addr.address
            netmask = addr.netmask

            # Skip loopback and interfaces with no netmask
            if not netmask or ip.startswith('127.'):
                continue

            ip_int  = struct.unpack('!I', socket.inet_aton(ip))[0]
            nm_int  = struct.unpack('!I', socket.inet_aton(netmask))[0]
            brd_int = (ip_int & nm_int) | (~nm_int & 0xFFFFFFFF)
            broadcast = socket.inet_ntoa(struct.pack('!I', brd_int))

            if broadcast not in seen:
                results.append((broadcast, ip))
                seen.add(broadcast)

    return results or [('255.255.255.255', '0.0.0.0')]
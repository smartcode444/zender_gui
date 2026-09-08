import socket
# import asyncio
import threading
import struct
import psutil
import os



class NetworkManager:
    def __init__(self, username: str):
        self.UDP_PORT          = 7007
        self.TCP_PORT          = 5005
        self.CTRL_PORT         = 5006
        self.INTERFACES        = get_all_broadcast_addresses()
        self.own_ips           = {ip for _, ip in self.INTERFACES}

        self.DISCOVERY_MESSAGE = "XENDER_DISCOVERY_REQUEST"
        self.RESPONSE_MESSAGE  = "I_SEE_U"

        self.name              = username

        self.tcp_send_server   = None
        self.tcp_recv_server   = None 
        self.tcp_send          = None
        self.tcp_recv          = None
        self.ctrl_socket       = None # accepted from broadcaster side
        self.ctrl_conn         = None # connected from scanner side
        # self._cancel_flag      = asyncio.Event()
        self._cancel_flag      = threading.Event()

        self.sending_status    = {"SUCCESS": 0, "LOADING": 1, "ERROR": 2}
        self.recieving_status  = {"SUCCESS": 0, "LOADING": 1, "ERROR": 2}
 
        self.selected_mode     = None

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 0)
        self.udp_socket.bind(('', self.UDP_PORT))
        self.udp_socket.settimeout(1.0)

    def init_bd_socks(self):
        """Intialize sockets for broadcasting."""
        self.selected_mode = "broadcast"

        self.ctrl_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.ctrl_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
        self.ctrl_server.bind(('', self.CTRL_PORT))
        self.ctrl_server.listen(1)

        self.tcp_send_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_send_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
        self.tcp_send_server.bind(('', self.TCP_PORT))
        self.tcp_send_server.listen(1)

        self.tcp_recv_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_recv_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  
        self.tcp_recv_server.bind(('', self.TCP_PORT))
        self.tcp_recv_server.listen(1)


    def broadcast(self, message) -> str:
        """Send discovery packet on every interface, and wait for a response"""
        for brd_addr, _ in self.INTERFACES:
            try:
                self.udp_socket.sendto(message, (brd_addr , self.UDP_PORT))
            except OSError:
                pass # Interface may be down or unreachable

        try:
            data, addr = self.udp_socket.recvfrom(1024)
            # print(data)
        except socket.timeout:
            raise

        # Filter out echoes of my own broadcast
        # if data.startswith(message):
        #     return None, None

        decoded = data.decode('utf-8').strip()   
        if decoded[:7] == "I_SEE_U":
            device_name = decoded[7:]
            return device_name, addr
        # elif decoded[-24:] == "XENDER_DISCOVERY_REQUEST":
        #     return "smartcode", addr

        return None, None

    
    def bd_connect(self):
        self.tcp_send_server.settimeout(1.0)   
        self.tcp_recv_server.settimeout(1.0)   
        self.ctrl_server.settimeout(1.0)   
        try:
            self.tcp_send, _ = self.tcp_send_server.accept()
            self.tcp_send_server.close()

            self.tcp_recv, _ = self.tcp_recv_server.accept()
            self.tcp_recv_server.close()

            self.ctrl_socket, _ = self.ctrl_server.accept()
            self.ctrl_server.close()

        except socket.timeout:
            raise socket.timeout 
    
        except Exception as e:
            self.tcp_send_server.close()
            raise e
    

    def init_scan_socks(self):
        """Intialize sockets for scanning."""
        self.selected_mode = "scan"
        self.tcp_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)        
        self.tcp_recv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)       
        self.ctrl_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        
    
    def scan(self, msg, devices) -> tuple:
        """Scan for devices on the network."""
        try:
            data, address = self.udp_socket.recvfrom(1024)
        except socket.timeout:
            raise socket.timeout

        sender_ip = address[0]
        if sender_ip in self.own_ips:
            return None, None
        
        username_len  = data[0]
        client_name   = data[1:1+username_len].decode('utf-8')
        client_msg    = data[1+username_len:].decode('utf-8')

        if client_msg == "XENDER_DISCOVERY_REQUEST" and client_name not in devices:
            try:
                self.udp_socket.sendto(msg, (address))
            except OSError:
                return None, None
            
            return client_name, address

        return None, None

    def sc_connect(self, device_addr):
        self.tcp_send.settimeout(1.0)
        self.tcp_recv.settimeout(1.0)
        self.ctrl_conn.settimeout(1.0)
        try:
            self.tcp_send.connect((device_addr, self.TCP_PORT))
            self.tcp_recv.connect((device_addr, self.TCP_PORT))
            self.ctrl_conn.connect((device_addr, self.CTRL_PORT))

        except socket.timeout:
            raise socket.timeout

        except Exception as e:
            raise e


    def cancel_transfer(self):
        self._cancel_flag.set()

    # <- SEND ->

    def _send_end(self):
        """Send END command."""        # Send a command byte: 0x02 means END
        try:
            self.tcp_send.send(b'\x02')
        except OSError:
            pass

    def ctrl_socket_obj(self):
        """Return the active control socket (broadcaster or scanner side)."""
        return getattr(self, 'ctrl_socket', None) or getattr(self, 'ctrl_conn', None)

    def send_cancel_signal(self):
        """Send a CANCEL byte (0x03) to the peer over the control channel.
        Silently ignore if no control channel is present or write fails.
        """
        ctrl = self.ctrl_socket_obj()
        if not ctrl:
            return
        try:
            ctrl.send(b'\x03')
        except OSError:
            pass

    def watch_for_cancel(self, poll_interval: float = 0.5):
        """Run in a background thread to watch the control channel for a CANCEL byte.

        When a CANCEL (0x03) is received from the peer the local `_cancel_flag` is set.
        This method intentionally returns quickly if there is no control socket.
        """
        ctrl = self.ctrl_socket_obj()
        if not ctrl:
            return

        try:
            ctrl.settimeout(poll_interval)
        except OSError:
            # If the socket is closed or doesn't support timeout, just return
            return

        try:
            while not self._cancel_flag.is_set():
                try:
                    data = ctrl.recv(1)
                    if data == b'\x03':
                        self._cancel_flag.set()
                        return
                    if not data:
                        # remote closed control socket
                        return
                except socket.timeout:
                    continue
                except OSError:
                    return
        finally:
            try:
                ctrl.settimeout(None)
            except Exception:
                pass

    # def send_cancel_signal(self):
    #     """Send a CANCEL byte to the other side via the control socket."""
    #     ctrl = self.ctrl_socket or self.ctrl_conn
    #     try:
    #         ctrl.send(b'\x03')
    #     except OSError:
    #         pass

    # def watch_for_cancel(self):
    #     """Runs concurrently during transfer
    #     Blocks until the other side sends 0x03, then sets _cancel_flag."""
    #     ctrl = self.ctrl_socket or self.ctrl_conn
    #     ctrl.setblocking(False)
    #     try:
    #         while True:
    #             data = ctrl.recv(1)
    #             if data == b'\x03':
    #                 self._cancel_flag.set()
    #                 return
    #     except OSError:
    #         raise

    def _send_file(self, name, path, progress_callback, rel_path: str = None):
        """Send a single file over the data socket.
        Arguments:
          - name: base filename shown to receiver
          - path: local filesystem path to read
          - progress_callback(progress: float, status: int)
          - rel_path: optional relative path to send instead of `name` (for folders)

        The header layout matches the receiver expected layout:
          [4B name_len][name_bytes][8B file_size]

        This method starts a small watcher thread that listens on the control
        channel for a CANCEL byte (0x03). If the local user cancels the transfer
        `cancel_transfer()` should be called which will cause this function to
        send a CANCEL byte to the peer and abort the transfer.
        """
        # Use the provided relative path name for header if given
        header_name = rel_path if rel_path is not None else name

        self._cancel_flag.clear()
        status = self.sending_status["LOADING"]

        filesize = os.path.getsize(path)
        namebytes = header_name.encode('utf-8')

        # build header: [4B name_len][name][8B file_size]
        header = len(namebytes).to_bytes(4, 'big') + namebytes + filesize.to_bytes(8, 'big')

        # Start a background watcher for remote CANCEL signals
        watcher = threading.Thread(target=self.watch_for_cancel, daemon=True)
        watcher.start()

        try:
            # send header (blocking)
            self.tcp_send.sendall(header)

            sent = 0
            with open(path, 'rb') as f:
                while sent < filesize:
                    if self._cancel_flag.is_set():
                        # local or remote requested cancel -> notify peer and abort
                        try:
                            self.send_cancel_signal()
                        except Exception:
                            pass
                        status = self.sending_status["ERROR"]
                        progress_callback(sent / max(1, filesize), status)
                        raise RuntimeError(f"[!] Transfer of '{name}' cancelled")

                    chunk = f.read(65536)
                    if not chunk:
                        break
                    try:
                        self.tcp_send.sendall(chunk)
                    except (OSError, BlockingIOError) as e:
                        status = self.sending_status["ERROR"]
                        progress_callback(sent / max(1, filesize), status)
                        # notify peer if possible then re-raise
                        try:
                            self.send_cancel_signal()
                        except Exception:
                            pass
                        raise RuntimeError(f"[!] Error while sending '{name}': {e}")

                    sent += len(chunk)
                    progress_callback(sent / filesize, status)

            status = self.sending_status["SUCCESS"]
            progress_callback(1.0, status)

        finally:
            try:
                # ensure watcher thread will exit soon
                self._cancel_flag.set()
            except Exception:
                pass

    def _send_folder(self, folder_path, progress_callback):
        # Build the list of files and total size for progress calculation
        folder_size = 0
        files_list = []  # list of tuples (full_path, rel_path)

        base_folder_name = os.path.basename(folder_path)
        for root_dir, _, files in os.walk(folder_path):
            for file in files:
                full_path = os.path.join(root_dir, file)
                rel_path = os.path.relpath(full_path, folder_path)
                rel_path = os.path.join(base_folder_name, rel_path).replace(os.sep, '/')
                files_list.append((full_path, rel_path))
                folder_size += os.path.getsize(full_path)

        # Informal progress: iterate through each file and send as _send_file
        total_sent = 0
        for full_path, rel_path in files_list:
            if self._cancel_flag.is_set():
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

    def send_folder(self, folder_path, progress_callback):
        """Public wrapper to send a folder (keeps API symmetry)."""
        return self._send_folder(folder_path, progress_callback)

    def send_file(self, path, name=None, progress_callback=None):
        """Public wrapper: send one file. `name` optional display name."""
        if name is None:
            name = os.path.basename(path)
        return self._send_file(name, path, progress_callback)


    def _recieve_file(self, dest_folder, progress_callback):
        """Receive files over the data socket. 
        Shuts down socket on cancellation so sender unblocks."""
        self.tcp_recv.setblocking(False)
        status = self.sending_status["LOADING"]

        def recv_exact(n: int) -> bytes:
            """Await exactly n bytes"""
            buf = b''
            while len(buf) < n:
                chunk = self.tcp_recv.recv(n - len(buf))
                if not chunk:
                    raise ConnectionError("[!] Connection closed by sender")
                buf += chunk
            return buf

        # Start watcher thread to detect remote CANCEL bytes on control channel
        watcher = threading.Thread(target=self.watch_for_cancel, daemon=True)
        watcher.start()

        try:
            name_len = int.from_bytes(recv_exact(4), 'big')
            filename = (recv_exact(name_len)).decode('utf-8')
            filesize = int.from_bytes(recv_exact(8), 'big')

            # Recieve file
            filepath = os.path.join(dest_folder, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "wb") as f:
                received = 0
                while received < filesize:
                    if self._cancel_flag.is_set():
                        status = self.recieving_status["ERROR"]
                        progress_callback(received / max(1, filesize), status)
                        raise RuntimeError("[!] Transfer cancelled by sender")

                    chunk = self.tcp_recv.recv(
                        min(65536, filesize - received)
                    )
                    if not chunk:
                        raise ConnectionError("\n[!] Connection lost mid-transfer")
                    f.write(chunk)
                    received += len(chunk)
                    progress_callback(received/filesize, status)

            status = self.sending_status["SUCCESS"]
            progress_callback(received/filesize, status)


        # except asyncio.CancelledError:
        #     self.send_cancel_signal()
        #     raise

        except (ConnectionError, BlockingIOError, RuntimeError) as e:
            status = self.recieving_status.get("ERROR", 2)
            # best-effort progress update
            try:
                progress_callback(0.0, status)
            except Exception:
                pass
            raise

        finally:
            try:
                self.tcp_recv.setblocking(True)
            except OSError:
                pass
        

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
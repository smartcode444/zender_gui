import socket
import select
import threading
import struct
import psutil
import os


class NetworkManager:
    def __init__(self, controller, username: str):
        self.controller        = controller

        self.UDP_PORT          = 7007
        self.TCP_PORT          = 5005
        self.CTRL_PORT         = 5006
        self.INTERFACES        = get_all_broadcast_addresses()
        self.own_ips           = {ip for _, ip in self.INTERFACES}

        self.DISCOVERY_MESSAGE = "XENDER_DISCOVERY_REQUEST"
        self.RESPONSE_MESSAGE  = "I_SEE_U"

        self.name              = username

        self.recv_filename     = None
        self.recv_foldername   = None

        self.tcp_send_server   = None
        self.tcp_recv_server   = None 
        self.tcp_send          = None
        self.tcp_recv          = None
        # self.ctrl_socket       = None # accepted from broadcaster side
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
        if data.startswith(message):
            return None, None

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

            self.ctrl_conn, _ = self.ctrl_server.accept()
            self.ctrl_server.close()

        except socket.timeout:
            raise socket.timeout 
    
        except Exception as e:
            self.tcp_send_server.close()
            raise e
    

    def init_scan_socks(self):
        """Intialize sockets for scanning."""
        self.selected_mode = "scan"
        self.tcp_send  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)        
        self.tcp_recv  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)       
        self.ctrl_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        
    
    def scan(self, msg, devices) -> tuple:
        """Scan for devices on the network."""
        try:
            data, address = self.udp_socket.recvfrom(1024)
            if not data:
                print("Recived nothing!!!")
            # print(f"Data: {data}") 
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
        print("Device address in sc_connect:", device_addr)
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


    def set_cancel_signal(self):
        self._cancel_flag.set()

    def clear_cancel_signal(self):
        self._cancel_flag.clear()

    # <- SEND ->

    def _send_end(self):
        """Send END command."""        # Send a command byte: 0x02 means END
        try:
            self.tcp_send.send(b'\x02')
        except OSError:
            pass

    # Sender side - on cancel, report exactly how many bytes it pushed into the socket
    def cancel_transfer(self, sent_so_far):
        """Send a CANCEL byte (0x03) and the amount of bytes pushed to the socket
        to the peer over the control channel.
        """
        try:
            # [Ox03][8B bytes_sent_into_socket]
            self.ctrl_conn.send(b'\x03' + sent_so_far.to_bytes(8, 'big'))
        except OSError:
            pass

    # Reciever side - Recieves cancel signal 
    def handle_cancel(self, reciever_recieved, timeout: float = 1.0):
        """Drain exactly remaining bytes from the data socket and discard"""
        try:
            self.ctrl_conn.settimeout(timeout) # Handle cancel process should be done within a second
        except OSError:
            # If the socket is closed or doesn't support timeout, just return
            return

        try:
            cmd = self.ctrl_conn.recv(1)
            if cmd == b'\x03':
                count_bytes = b''
                while len(count_bytes) < 8:
                    chunk = self.ctrl_conn.recv(8 - len(count_bytes))
                    if not chunk:
                        break
                    count_bytes += chunk

                sender_sent = int.from_bytes(count_bytes, 'big')

                gap = sender_sent - reciever_recieved
                discarded = 0
                while discarded < gap:
                    chunk = self.tcp_recv.recv(min(65536, gap - discarded))
                    if not chunk:
                        break
                    discarded += len(chunk)

        except Exception:
            raise


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

        self.clear_cancel_signal()    # <--------
        status = self.sending_status["LOADING"]

        filesize = os.path.getsize(path)
        namebytes = header_name.encode('utf-8')

        # build header: [4B name_len][name][8B file_size]
        header = len(namebytes).to_bytes(4, 'big') + namebytes + filesize.to_bytes(8, 'big')

        try:
            # send header (blocking)
            self.tcp_send.sendall(header)

            sent = 0
            with open(path, 'rb') as f:
                while sent < filesize:
                    # _, writable, _ = select.select([], [self.ctrl_conn], [], 0.1)
                    # if writable:
                    #     self.cancel_transfer()
                    #     return

                    if self._cancel_flag.is_set():
                        # local or remote requested cancel -> notify peer and abort
                        try:
                            self.cancel_transfer(sent)
                            return
                        except Exception:
                            pass
                        status = self.sending_status["ERROR"]
                        progress_callback(sent / max(1, filesize), status)
                        return

                    # Send next chunk
                    chunk = f.read(65536)
                    if not chunk:
                        break

                    try:
                        self.tcp_send.sendall(chunk)
                    except (OSError, BlockingIOError) as e:
                        print("Network error - connection lost")
                        status = self.sending_status["ERROR"]
                        progress_callback(sent / max(1, filesize), status)
                        # notify peer if possible then re-raise
                        try:
                            self.cancel_transfer(sent)
                            return
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
                self.set_cancel_signal()
            except Exception as e:
                pass

            
    def _send_folder(self, folder_path, progress_callback):
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

        # Send folder size to reciever
        self.tcp_send.send(folder_size.to_bytes(8, 'big'))

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
        return self._send_folder(folder_path, progress_callback)

    def send_file(self, path, name=None, progress_callback=None):
        if name is None:
            name = os.path.basename(path)
        return self._send_file(name, path, progress_callback)


    def _recieve_file(self, dest_folder):
        """Receive files over the data socket. 
        Shuts down socket on cancellation so sender unblocks."""
        self.clear_cancel_signal()
        status = self.sending_status["LOADING"]

        def recv_exact(n: int) -> bytes:
            """Await exactly n bytes"""
            buf = b''
            while len(buf) < n:
                readable, _, _ = select.selct([self.tcp_recv, self.ctrl_conn], [], [], 5.0)
                
                if not readable:
                    self.handle_cancel(len(buf))
                    raise RuntimeError("Timeout - connection may be dead")

                if self.ctrl_conn in readable:
                    self.handle_cancel(len(buf))
                    raise RuntimeError("Sender cancelled - Stop recieving")

                if self._cancel_flag.is_set():
                    self.handle_cancel(len(buf))
                    raise RuntimeError("[!] Transfer cancelled by sender")

                if self.tcp_recv in readable:
                    chunk = self.tcp_recv.recv(n - len(buf))
                    if not chunk:
                        raise ConnectionError("[!] Connection closed by sender")
                    buf += chunk
                return buf

        try:
            recieved = 0
            name_len = int.from_bytes(recv_exact(4), 'big')
            filename = (recv_exact(name_len)).decode('utf-8')

            progress_callback = self.controller.register_incoming_transfer(filename)

            filesize = int.from_bytes(recv_exact(8), 'big')

            # Recieve file
            filepath = os.path.join(dest_folder, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "wb") as f:
                while recieved < filesize:
                    # Watch Both sockets simultaneously 
                    readable, _, _ = select.select([self.tcp_recv, self.ctrl_conn], [], [], 5.0)

                    if not readable:
                        self.handle_cancel(recieved)
                        raise RuntimeError("Timeout - connection may be dead")

                    if self.ctrl_conn in readable:
                        self.handle_cancel(recieved)
                        raise RuntimeError("Sender cancelled - Stop recieving")

                    if self._cancel_flag.is_set():
                        self.handle_cancel(recieved)
                        raise RuntimeError("[!] Transfer cancelled by sender")

                    if self.tcp_recv in readable:
                        chunk = self.tcp_recv.recv(
                            min(65536, filesize - recieved)
                        )
                        if not chunk:
                            raise ConnectionError("\n[!] Connection lost mid-transfer")
                        f.write(chunk)
                        recieved += len(chunk)
                        progress_callback(recieved/filesize, status)

            status = self.sending_status["SUCCESS"]
            progress_callback(recieved/filesize, status)

        except (ConnectionError, BlockingIOError, RuntimeError) as e:
            status = self.recieving_status.get("ERROR", 2)
            # best-effort progress update
            try:
                progress_callback(recieved / max(1, filesize), status)
            except Exception:
                pass
            raise

        
    def _recieve_folder(self, dest_folder):
        """Recieve folders"""
        # self.tcp_recv.setblocking(False)
        self.clear_cancel_signal()
        status = self.sending_status["LOADING"]

        def recv_exact(n: int) -> bytes:
            """Await exactly n bytes"""
            buf = b''
            while len(buf) < n:
                readable, _, _ = select.select([self.tcp_recv, self.ctrl_conn], [], [], 5.0)
                
                if not readable:
                    self.handle_cancel(len(buf))
                    raise RuntimeError("Timeout - connection may be dead")

                if self.ctrl_conn in readable:
                    self.handle_cancel(len(buf))
                    raise RuntimeError("Sender cancelled - Stop recieving")

                if self._cancel_flag.is_set():
                    self.handle_cancel(len(buf))
                    raise RuntimeError("[!] Transfer cancelled by sender")

                if self.tcp_recv in readable:
                    chunk = self.tcp_recv.recv(n - len(buf))
                    if not chunk:
                        raise ConnectionError("[!] Connection closed by sender")
                    buf += chunk
                return buf

        folder_size = int.from_bytes(recv_exact(8), 'big')

        # Need - filename, - recieved_bytes
        recieved_folder = 0 # Total bytes recieved in sent folder
        while recieved_folder < folder_size: 
            try:
                path_name_len = int.from_bytes(recv_exact(4), 'big')
                relpath = (recv_exact(path_name_len)).decode('utf-8')

                folder_name = os.path.basename(relpath)
                progress_callback = self.controller.register_incoming_transfer(folder_name)

                filesize = int.from_bytes(recv_exact(8), 'big')

                # Recieve file
                filepath = os.path.join(dest_folder, relpath)
                os.makedirs(os.path.dirname(filepath), exist_ok=True) # Recreate directory structure in reciever end

                with open(filepath, "wb") as f:
                    recieved_file = 0

                    while recieved_file < filesize:
                        # Watch Both sockets simultaneously 
                        readable, _, _ = select.selct([self.tcp_recv, self.ctrl_conn], [], [], 5.0)

                        if not readable:
                            self.handle_cancel(recieved_file)
                            raise RuntimeError("Timeout - connection may be dead")

                        if self.ctrl_conn in readable:
                            self.handle_cancel(recieved_file)
                            raise RuntimeError("Sender cancelled - Stop recieving")

                        if self._cancel_flag.is_set():
                            self.handle_cancel(recieved_file)
                            raise RuntimeError("[!] Transfer cancelled by sender")

                        if self.tcp_recv in readable:
                            chunk = self.tcp_recv.recv(
                                min(65536, filesize - recieved_file)
                            )
                            if not chunk:
                                raise ConnectionError("\n[!] Connection lost mid-transfer")
                            f.write(chunk)
                            recieved_file += len(chunk)
                            progress_callback(recieved_folder/folder_size, status)


                status = self.sending_status["SUCCESS"]
                progress_callback(recieved_folder/folder_size, status)

            except (ConnectionError, BlockingIOError, RuntimeError) as e:
                status = self.recieving_status.get("ERROR", 2)
                # best-effort progress update
                try:
                    progress_callback(recieved_folder / max(1, folder_size), status)
                except Exception:
                    pass
                raise


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
import socket
import asyncio
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
        self._cancel_flag      = asyncio.Event()

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


    # <- SEND ->

    def _send_end(self):
        """Send END command."""        # Send a command byte: 0x02 means END
        try:
            self.tcp_client_socket.send(b'\x02')
        except OSError:
            pass

    async def _send_file(self, name, path, rel_path, progress_callback):
        """Send a single file over the data socket. 
            Notifies reciever on cancellation.     """
        loop = asyncio.get_event_loop()
        self.tcp_client_socket.setblocking(False)
        self._cancel_flag.clear()

        try:
            filesize = os.path.getsize(path)
            namebytes = rel_path.encode('utf-8') if rel_path else name.encode('utf-8')
            # ── Header
            # [0x01][4B name_len][name][8B file_size]
            # 8 bytes for file_size supports files up to 16 exabytes
            header = (b'\x01' 
                        + len(namebytes).to_bytes(4, 'big') 
                        + namebytes
                        + filesize.to_bytes(8, 'big'))
            await loop.sock_sendall(self.tcp_client_socket, header)

            with open(path, "rb") as f:
                sent = 0
                while sent < filesize:
                    if self._cancel_flag.is_set(): # Check between chunks
                        break
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    await loop.sock_sendall(self.tcp_client_socket, chunk)
                    sent += len(chunk)
                    progress_callback(sent/filesize)
                        

        except asyncio.CancelledError:
            self.send_cancel_signal()
            raise

        except OSError as e:
            raise RuntimeError(f"\n[!] Error while sending '{name}': {e}")

        finally:
            try:
                self.tcp_client_socket.setblocking(True)
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
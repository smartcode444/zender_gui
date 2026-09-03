import asyncio
import threading
# import queue
import socket
import os
from dataclasses import dataclass
from tkinter import filedialog
from src.model import NetworkManager
# from model import NetworkManager

@dataclass
class FileInfo:
    name: str          
    relative_path: str  

async def async_key_pressed():
    pass

class XenderController():
    def __init__(self, app, username):
        self.model = NetworkManager(username)
        self.app = app
        self.username = username
        self.running = True
        self.stop_scanning = threading.Event()
        self.stop_broadcasting = threading.Event()
        # self.scanned_devices = queue.Queue()
        # self.scanners = queue.Queue()
        self.scanned_devices = None
        self.scanners = None
        
    def scan(self):
        devices = {}
        print(f"Scanning... {len(devices)} found")
        self.model.init_scan_socks()

        print("Scanning for devices...\n")

        msg = b"I_SEE_U" + self.username.encode('utf-8')
        while not self.stop_scanning.is_set():
            try:
                name, addr = self.model.scan(msg, devices)
                if name:
                    devices[name] = addr
                    print(f"Scanning... {len(devices)} found")
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[!]  Error scanning: {e}")
                break

            if devices and self.scanned_devices != devices:
                self.scanned_devices = devices
                self.app.refresh_scan_devices(self.scanned_devices)

        # while not self.stop_scanning.is_set():
        #     self.scanned_devices.put({"Roomate-PC": ("127.0.0.1", "8080"), "Mackbook Alpha": ("127.0.0.2", "7070")})


    def run_scan(self):
        self.stop_scanning.clear()
        scan_thread = threading.Thread(target=self.scan)
        scan_thread.start()


        # while not self.stop_scanning.is_set():
        #     try:
        #         self.app.refresh_scan_devices(self.scanned_devices.get_nowait())
        #     except queue.Empty:
        #         continue

    def end_scan(self):
        self.stop_scanning.set()

    def broadcast(self):
        self.stop_broadcasting.clear()
        self.model.init_bd_socks()

        username_bytes = self.username.encode('utf-8')
        message = bytes([len(username_bytes)]) + username_bytes + b"XENDER_DISCOVERY_REQUEST"

        while not self.stop_broadcasting.is_set():
            print("broadcasting...")
            try:
                dev = self.model.broadcast(message)
                if dev:
                    print(dev)
            except socket.timeout:
                continue

            if dev and self.scanners !=  dev:
                self.scanners = dev
                self.app.refresh_scanners(self.scanners)


        # while self.stop_broadcasting.is_set():
        #     self.scanners.put("HP Probook")

    def run_broadcast(self):
        self.stop_broadcasting.set()
        broadcast_thread = threading.Thread(target=self.broadcast)
        broadcast_thread.start()
        # while not self.stop_broadcasting.is_set():
        #     try:
        #         self.app.refresh_scan_devices(self.scanned_devices.get_nowait())
        #     except queue.Empty:
        #         continue

    def end_broadcast(self):
        self.stop_broadcasting.set()


# <-- REST OF THE CODE IS NOT MEANT TO BE USED -->


    async def run(self):
        while self.running:
            choice = self.view.show_menu()
            if choice == '1':
                self.model.init_scan_socks()
                devices = {}
                msg    = b"I_SEE_U" + self.username.encode('utf-8')
                spin_i = 0

                # print("Scanning for devices... press 'q' to stop\n")

                msg = b"I_SEE_U" + self.username.encode('utf-8')
                while True:
                    # self.view.show_inline(
                    #     f"{SPINNER[spin_i % len(SPINNER)]}  Scanning... {len(devices)} found"
                    # )
                    # spin_i += 1  

                    # Check key (non-blocking)
                    user_key = self.view.get_input()
                    if user_key == 'q':
                        self.view.end_inline()        # move off spinner line
                        # print("[!]  Scanning stopped.")
                        break

                    try:
                        name, addr = self.model.scan(msg, devices)
                        if name:
                            devices[name] = addr
                            # Don't show_message here — breaks the spinner line
                    except socket.timeout:
                        continue
                    except Exception as e:
                        self.view.end_inline()
                        # print(f"[!]  Error scanning: {e}")
                        break

                self.view.end_inline()
                self.view.show_devices(devices)

                if devices:
                    idx = self.view.get_selection(len(devices))
                    if idx == 'q':
                        continue
                    selected_name = list(devices.keys())[idx-1]
                    # print(f"Connecting to {selected_name}... (press 'q' to stop)")
                    while True:
                        user_key = self.view.get_input()
                        if user_key == 'q':
                            # print("\n[!] User refused connection")
                            break
                        try:
                            self.model.sc_connect(devices[selected_name][0])
                            # print(Back.GREEN + f"[OK] Succesfully connected to {selected_name}")
                            await self.transfer_loop()
                            break
                        except socket.timeout:
                            continue
                        except Exception as e:
                            # print(f"[!] Error connecting to {selected_name}: {e}.")
                            break


            elif choice and choice == '2':
                # print("\nBroadcasting to network... (press q to stop)")
                self.model.init_bd_socks()
                username_bytes = self.username.encode('utf-8')
                message = bytes([len(username_bytes)]) + username_bytes + b"XENDER_DISCOVERY_REQUEST"
                spin_i = 0

                while True:
                    self.view.show_inline(
                        # f"{SPINNER[spin_i % len(SPINNER)]}  Broadcasting... (press q to stop)"
                    )
                    spin_i += 1  
                    user_key = self.view.get_input()
                    if user_key == 'q':
                        # print("\nBroadcasting stopped by user")
                        break
                    try:
                        if conn := self.model.broadcast(message):
                            # choice = self.view.ask_yes_no(f"\nAccept connection from '{conn}'")
                            if   choice == "1":
                                break
                            elif choice == "2":
                                # print("[!] User refused connection.")
                                # print("\nBroadcasting to network... (press q to go back to previous menu)")
                                continue                                
                        else:
                            continue
                    except socket.timeout:
                        continue

                # Connect to device
                if choice == "1":
                    # print(f"Connecting to {conn}... (press 'q' to stop)")
                    while True:
                        user_key = self.view.get_input()
                        if user_key == "q":
                            # print("\n[!] Connection stopped by user")
                            break
                        try:
                            self.model.bd_connect()
                            await self.transfer_loop()
                            break
                        except socket.timeout:
                            continue
                        except Exception as e:
                            # print(f"[!] Error connecting to {conn}: {e}.")
                            break
                    

            elif choice and choice == '3':
                break

    async def try_send_files(self):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        files = filedialog.askopenfilenames(
            title="Zender -> Select files", initialdir=downloads_dir, parent=self.window
        )
        if not files:
            # print("No files selected.")
            return
        
        file_paths = sorted([
            FileInfo(
                name=os.path.normpath(f),
                relative_path=None          # flat file, no folder structure
            )
            for f in files
        ])

        await self.try_send(file_paths)


    def ask_open_dirnames(self, title, initialdir):
        """
        Keeps prompting askdirectory until the user cancels, collecting one folder per dialog.
        """
        folders = []
        while True:
            prompt = f"{title}  [{len(folders)} selected — Cancel when done]"
            folder = filedialog.askdirectory(
                title=prompt,
                initialdir=initialdir or os.path.expanduser("~"),
                parent=self.window,
                mustexist=True,
            )
            if not folder:          # user hit Cancel — we're done
                break
            folder = os.path.normpath(folder)
            if folder not in folders:
                folders.append(folder)
        return folders

    async def try_send_folders(self):
        downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
        folders = self.ask_open_dirnames(title="Select Folders to Send", initialdir=downloads_dir)

        if not folders:
            # print("No folders selected.")
            return

        rel_file_paths = []

        # Process each selected directory
        for folder_path in sorted(folders):
            normalized_folder = os.path.normpath(folder_path)
            folder_files = []
            
            # Traverse folder contents
            for self.window_dir, _, files in os.walk(normalized_folder):
                for file in files:
                    full_path = os.path.join(self.window_dir, file)
                    folder_name = os.path.basename(normalized_folder)
                    rel_path    = os.path.join(folder_name, os.path.relpath(full_path, normalized_folder))
                    rel_path = rel_path.replace(os.sep, '/')
                    file_object = FileInfo(name=full_path, relative_path=rel_path)
                    folder_files.append(file_object)
            
            # Store paths if folder is not empty, otherwise ignore it
            if folder_files:
                rel_file_paths.extend(folder_files)

        # Sort the final flat list for consistency
        rel_file_paths.sort(key=lambda file_obj: file_obj.relative_path)

        await self.try_send(rel_file_paths)

    async def try_send(self, file_paths: list):
        """Send files"""
        cancelled = False
        # file_paths = filedialog.askopenfiles(
        #     title="Xender -> Select files",
        # )
        # if not file_paths:
        #     print("No files selected.")
        #     return

        # Cannot send more than 99 files
        no_files = len(file_paths)
        if   1 <= no_files < 9:
            no_files_bytes = ("0"+str(no_files)).encode('utf-8')
        elif 10 <= no_files <= 99:
            no_files_bytes = str(no_files).encode('utf-8')
        else:
            # print("Too many files selected (max 99).")
            return
        self.model.send_bytes(no_files_bytes)

        for file_obj in file_paths:
            name = os.path.basename(file_obj.name)
            path = file_obj.name
            filesize = os.path.getsize(file_obj.name)
            print(f"\n[>>] Sending '{name}' ({filesize / (1024*1024):.1f} MB) (press 'q' to stop)")

            if file_obj.relative_path: rel_path = file_obj.relative_path
            else: rel_path = None

            try:
                send_file = asyncio.create_task(
                    self.model._send_file(
                        name, path, rel_path,
                        on_progress=lambda s, t, st: self.view.show_progress(s, t, name, st)
                    )
                )
                pressed_key = asyncio.create_task(async_key_pressed())
                watch_cancel = asyncio.create_task(self.model.watch_for_cancel())

                done, pending = await asyncio.wait(
                    {send_file, pressed_key, watch_cancel}, 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass

                if send_file in done:
                    result = send_file.result()
                    self.view.end_inline()      # move off progress bar line
                    print(result)
                    
                elif pressed_key in done:
                    # print("[!] Sending stopped by user.")
                    cancelled = True
                    break

                elif watch_cancel in done:
                    # print("[!] Transfer cancelled by reciever.")
                    cancelled = True 
                    break

            except Exception as e:
                # print(f"[!] Error sending {name}: {e}")
                continue

        if not cancelled:
            self.model._send_end()

    async def try_recieve(self):
        """Recieve files"""

        # Prompt user for destination folder
        dest_folder = filedialog.askdirectory(title="Select destination folder", parent=self.window)
        if not dest_folder:
            # print(f"[!] Invalid Destination folder")
            return
        # print(f"{Back.GREEN}[OK] Destination folder {dest_folder}")

        # Async file count receive — user can cancel while waiting

        try:
            count_task  = asyncio.create_task(self.modelrecv_bytes(2))
            cancel_task = asyncio.create_task(async_key_pressed())

            done, pending = await asyncio.wait(
                {count_task, cancel_task},
                return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

            if cancel_task in done:
                # print("[!] Cancelled — returning to menu.")
                return

            no_files = count_task.result()
            print(f"  Receiving {no_files} file(s)...")

        except Exception as e:
            # print(f"[!] Failed to read file count: {e}")
            return
        finally:
            self.model.tcp_client_socket.setblocking(True)
        
        # Main Recieve
        
        try:
            recv_file = asyncio.create_task(
                self.model._recieve_file(
                    dest_folder,
                    on_progress=lambda s, t, st: self.view.show_progress(s, t, "receiving", st)
                )
            )
            pressed_key = asyncio.create_task(async_key_pressed())
            watch_cancel = asyncio.create_task(self.model.watch_for_cancel())

            done, pending = await asyncio.wait(
                {recv_file, pressed_key, watch_cancel}, 
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

            if recv_file in done:
                self.view.end_inline()                  # move off progress bar line
                print(recv_file.result())
                
            elif pressed_key in done:
                self.model.send_cancel_signal()
                # print("[!] Recieving stopped by user.")

            elif watch_cancel in done:
                # print("[!]  Transfer cancelled by sender.")
                return

        except Exception as e:
            # print(f"[!] Error: {e}")
            return


    async def transfer_loop(self):
        """Transfer files"""
        while True:
            sel = self.view.show_transfer_menu()
            # sel = self.view.get_selection(3)
            if   sel == 1:
                await self.try_send_files()    
            elif sel == 2:
                await self.try_send_folders()
            elif sel == 3:
                await self.try_recieve()
            elif sel == 4:break
            
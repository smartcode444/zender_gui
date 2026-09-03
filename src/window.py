import customtkinter as ctk
import sys
# import queue
from tkinter import filedialog
# from controller import XenderController
from src.controller import XenderController

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")


class P2PApp(ctk.CTk):
    def __init__(self, username):
        super().__init__()
        self.controller = XenderController(self, username)
        self.title("ZENDER - P2P File Transfer")
        self.geometry("950x620")
        self.minsize(850, 500)

        # Layout Configuration
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)

        # Connected States
        self.connected_user = ""

        # ----------------- SIDEBAR -----------------
        self.sidebar = ctk.CTkFrame(self, width=220, corner_radius=0)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.sidebar.grid_rowconfigure(4, weight=1)

        self.app_title = ctk.CTkLabel(
            self.sidebar, text="P2P Transfer", font=ctk.CTkFont(size=20, weight="bold")
        )
        self.app_title.grid(row=0, column=0, padx=20, pady=(25, 20))

        self.btn_pc_pc = ctk.CTkButton(
            self.sidebar,
            text="💻  PC-PC Transfer",
            anchor="w",
            height=40,
            command=self.show_main_selection,    # Call show main_selection
        )
        self.btn_pc_pc.grid(row=1, column=0, padx=15, pady=8, sticky="ew")

        self.btn_phone_pc = ctk.CTkButton(
            self.sidebar,
            text="📱  Phone-PC (Disabled)",
            anchor="w",
            height=40,
            state="disabled",
            fg_color="#2b2b2b",
            text_color_disabled="#666666",
        )
        self.btn_phone_pc.grid(row=2, column=0, padx=15, pady=8, sticky="ew")

        # ----------------- MAIN VIEW CONTAINER -----------------
        self.main_container = ctk.CTkFrame(self, corner_radius=0, fg_color="transparent")
        self.main_container.grid(row=0, column=1, sticky="nsew", padx=20, pady=20)
        self.main_container.grid_rowconfigure(0, weight=1)
        self.main_container.grid_columnconfigure(0, weight=1)

        # Initialize Views
        self.frames = {}
        for FrameClass in (
            MainSelectionView,
            ScanningView,
            BroadcastingView,
            ConnectedTransferView,
        ):
            frame = FrameClass(self.main_container, self)
            self.frames[FrameClass] = frame
            frame.grid(row=0, column=0, sticky="nsew")

        self.show_main_selection()

    def show_view(self, frame_class):
        frame = self.frames[frame_class]
        frame.tkraise()
        if hasattr(frame, "on_show"):
            frame.on_show()

    def show_main_selection(self):
        self.show_view(MainSelectionView)

    def set_connected_user(self, mode, username, dev_addr):
        self.controller.connect(dev_addr)   # <- Intiate connection
        # self.connected_user = username
        transfer_view = self.frames[ConnectedTransferView]
        transfer_view.update_header(username)
        self.show_view(ConnectedTransferView)

    def refresh_scan_devices(self, devices):
        devices_list = []
        for name, addr in devices.items():
            devices_list.append(name + " " + (addr[0]))
        self.frames[ScanningView].display_devices(devices)

    def refresh_scanners(self, device):
        self.frames[BroadcastingView].display_device(device)

# -------------------------------------------------------------
#  MAIN SELECTION VIEW (Scan vs Broadcast)
# -------------------------------------------------------------
class MainSelectionView(ctk.CTkFrame):
    def __init__(self, parent, app: P2PApp):
        super().__init__(parent, fg_color="transparent")
        self.app = app
        self.grid_columnconfigure((0, 1), weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.scan_card = ctk.CTkButton(
            self,
            text="🎯\n\nScan for Devices\n\nFind available peers on your local network",
            font=ctk.CTkFont(size=16, weight="bold"),
            corner_radius=12,
            command=self.on_scan_card,   
        )
        self.scan_card.grid(row=0, column=0, padx=20, pady=40, sticky="nsew")

        self.broadcast_card = ctk.CTkButton(
            self,
            text="📡\n\nBroadcast Availability\n\nMake your device visible to other peers",
            font=ctk.CTkFont(size=16, weight="bold"),
            corner_radius=12,
            command=self.on_broadcast_card,
        )
        self.broadcast_card.grid(row=0, column=1, padx=20, pady=40, sticky="nsew")

    def on_scan_card(self):
        """Switch from main view to scanning view
            and call controller to start scanning"""
        self.app.controller.run_scan()
        self.app.show_view(ScanningView)

    def on_broadcast_card(self):
        """Switch from main view to broadcasting view
            and call the controller to start broadcasting"""
        app.controller.run_broadcast()
        self.app.show_view(BroadcastingView)


# -------------------------------------------------------------
# SCANNING VIEW
# -------------------------------------------------------------
class ScanningView(ctk.CTkFrame):
    def __init__(self, parent, app: P2PApp):
        super().__init__(parent, fg_color="transparent")
        self.app = app
        self.device_rows = []
        self.devices = {}

        self.header = ctk.CTkLabel(self, text="Scanning for Devices...", font=ctk.CTkFont(size=22, weight="bold"))
        self.header.pack(pady=(10, 5), anchor="w")

        self.subtitle = ctk.CTkLabel(self, text="Select an available peer to initiate connection", text_color="gray")
        self.subtitle.pack(pady=(0, 15), anchor="w")

        self.device_list_frame = ctk.CTkScrollableFrame(self, corner_radius=8)
        self.device_list_frame.pack(fill="both", expand=True, pady=10)

    def display_devices(self, devices): 
        self.devices = devices

    def on_show(self):
        self.clear_devices()
        # devices = ["Laptop-Beta (192.168.1.12)", "Workstation-Gamma (192.168.1.45)", "NUC-Delta (192.168.1.89)"]
        # for dev in devices:
        #     self.add_discovered_device(dev)
        if self.devices:
            for dev_name, dev_addr in self.devices.items():
                self.add_discovered_device((dev_name, dev_addr))

    def clear_devices(self):
        for widget in self.device_list_frame.winfo_children():
            widget.destroy()
        self.device_rows.clear()

    def add_discovered_device(self, dev_info: tuple[str, str]):
        dev_name = dev_info[0]
        dev_addr = dev_info[1]

        row = ctk.CTkFrame(self.device_list_frame)
        row.pack(fill="x", pady=5, padx=5)

        lbl = ctk.CTkLabel(row, text=f"💻  {dev_name[0]} ({dev_addr})", font=ctk.CTkFont(size=14))
        lbl.pack(side="left", padx=15, pady=12)

        btn = ctk.CTkButton(
            row, text="Connect", width=100
        )
        btn.configure(command=lambda b=btn, name=dev_name, addr=dev_addr: self.handle_connect(b, name, addr))
        btn.pack(side="right", padx=15, pady=12)

        self.device_rows.append({"frame": row, "button": btn, "name": dev_name})

    def handle_connect(self, active_btn, dev_name, dev_addr):
        for item in self.device_rows:
            if item["button"] != active_btn:
                item["button"].configure(state="disabled")
            else:
                item["button"].configure(state="disabled", fg_color="#0082FC")
                # item["frame"].configure(text="Connecting...", fg_color="#4C02F8")
        self.after(600, lambda: self.app.set_connected_user("scan", dev_name, dev_addr))
        

# -------------------------------------------------------------
# 3. BROADCASTING VIEW
# -------------------------------------------------------------
class BroadcastingView(ctk.CTkFrame):
    def __init__(self, parent, app: P2PApp):
        super().__init__(parent, fg_color="transparent")
        self.app = app
        self.device = None

        self.header = ctk.CTkLabel(self, text="Broadcasting...", font=ctk.CTkFont(size=22, weight="bold"))
        self.header.pack(pady=(10, 5), anchor="w")

        self.status_lbl = ctk.CTkLabel(self, text="Waiting for incoming connection requests...", text_color="gray")
        self.status_lbl.pack(pady=(0, 15), anchor="w")

        self.incoming_list_frame = ctk.CTkScrollableFrame(self, corner_radius=8)
        self.incoming_list_frame.pack(fill="both", expand=True, pady=10)

    def display_device(self, device):
        self.device = device

    def on_show(self):
        print(self.device)
        for widget in self.incoming_list_frame.winfo_children():
            widget.destroy()
        if self.device:
            print(self.device)
            self.add_incoming_request(self.device)

    def add_incoming_request(self, requester_info: tuple[str, str]):
        requester_name = requester_info[0]
        requester_addr = requester_info[1][0]
        requester_port = requester_info[1][1]
        row = ctk.CTkFrame(self.incoming_list_frame)
        row.pack(fill="x", pady=5, padx=5)

        lbl = ctk.CTkLabel(row, text=f"📥 Incoming: {requester_name} ({requester_addr}:{requester_port})", font=ctk.CTkFont(size=14))
        lbl.pack(side="left", padx=15, pady=12)

        btn_accept = ctk.CTkButton(
            row, text="Accept", width=100,
            command=lambda: self.app.set_connected_user(requester_name, requester_addr),
        )
        btn_accept.pack(side="right", padx=15, pady=12)


# -------------------------------------------------------------
# 4. CONNECTED TRANSFER VIEW (Tabs for Sending & Receiving)
# -------------------------------------------------------------
class ConnectedTransferView(ctk.CTkFrame):
    def __init__(self, parent, app: P2PApp):
        super().__init__(parent, fg_color="transparent")
        self.app = app

        # Header Status
        self.lbl_connected = ctk.CTkLabel(
            self, text="Connected to: None", font=ctk.CTkFont(size=22, weight="bold")
        )
        self.lbl_connected.pack(pady=(5, 15), anchor="w")

        # Tab View Setup
        self.tabview = ctk.CTkTabview(self)
        self.tabview.pack(fill="both", expand=True)

        self.tab_send = self.tabview.add("Sending")
        self.tab_recv = self.tabview.add("Receiving")

        # ================= SENDING TAB =================
        self.action_bar = ctk.CTkFrame(self.tab_send, fg_color="transparent")
        self.action_bar.pack(fill="x", pady=(10, 15))

        self.btn_send_files = ctk.CTkButton(
            self.action_bar, text="📁  Send Files", width=140, height=36, command=self.action_send_files
        )
        self.btn_send_files.pack(side="left", padx=(0, 10))

        self.btn_send_folder = ctk.CTkButton(
            self.action_bar, text="📂  Send Folder", width=140, height=36, command=self.action_send_folder
        )
        self.btn_send_folder.pack(side="left")

        self.send_list_frame = ctk.CTkScrollableFrame(self.tab_send, label_text="Outgoing Transfers")
        self.send_list_frame.pack(fill="both", expand=True)

        # ================= RECEIVING TAB =================
        self.recv_list_frame = ctk.CTkScrollableFrame(self.tab_recv, label_text="Incoming Transfers")
        self.recv_list_frame.pack(fill="both", expand=True)

    def on_show(self):
        # Mock receiving an incoming file when the view is opened
        self.add_transfer_item(self.recv_list_frame, "Vacation_Photos.zip", 0.15)
        self.add_transfer_item(self.recv_list_frame, "Project_Document.pdf", 0.88)

    def update_header(self, username: str):
        self.lbl_connected.configure(text=f"Connected to: {username}")
        # Make sure we start on the sending tab by default
        self.tabview.set("Sending")

    def action_send_files(self):
        filepaths = filedialog.askopenfilenames(title="Select Files to Send")
        for path in filepaths:
            filename = path.split("/")[-1]
            self.add_transfer_item(self.send_list_frame, filename, 0.0)

    def action_send_folder(self):
        folderpath = filedialog.askdirectory(title="Select Folder to Send")
        if folderpath:
            foldername = folderpath.split("/")[-1]
            self.add_transfer_item(self.send_list_frame, f"Folder: {foldername}", 0.0)

    def add_transfer_item(self, parent_frame, item_name: str, progress_val: float = 0.0):
        """Reusable method to add UI items to either the Send or Receive lists"""
        row = ctk.CTkFrame(parent_frame)
        row.pack(fill="x", pady=5, padx=5)

        lbl = ctk.CTkLabel(row, text=item_name, width=220, anchor="w")
        lbl.pack(side="left", padx=10, pady=10)

        progress = ctk.CTkProgressBar(row, width=220)
        progress.set(progress_val)
        progress.pack(side="left", padx=10, pady=10)

        pct_lbl = ctk.CTkLabel(row, text=f"{int(progress_val * 100)}%", width=45)
        pct_lbl.pack(side="left", padx=5)

        btn_cancel = ctk.CTkButton(
            row,
            text="Cancel",
            width=70,
            fg_color="#a83232",
            hover_color="#7a2424",
            command=lambda: row.destroy(),  # Hook up backend abort signal here
        )
        btn_cancel.pack(side="right", padx=10, pady=10)


if __name__ == "__main__":
    app = P2PApp("smartcode")
    app.mainloop()
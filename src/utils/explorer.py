import tkinter as tk
from tkinter import ttk
import json
import threading

class BlockchainGUI:
    def __init__(self):
        self.columns = []              
        self.tx_columns = []           
        self.blocks = []               
        self.transactions = []         

        self.seen_hashes = set()       
        self.seen_tx_ids = set()       

        self.latest_state = None       
        self.last_state_hash = None 

        self.lock = threading.Lock()

        self.gui_thread = threading.Thread(target=self._init_gui, daemon=True)
        self.gui_thread.start()

    def _init_gui(self):
        self.root = tk.Tk()
        self.root.title("Blockchain Viewer")
        self.root.geometry("1900x400")

        # Notebook (Tabs)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Blocks Tab
        self.block_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.block_frame, text="Blocks")
        self.tree_blocks = ttk.Treeview(self.block_frame, show="headings")
        self.tree_blocks.pack(fill=tk.BOTH, expand=True)

        # Transactions Tab
        self.tx_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.tx_frame, text="Transactions")
        self.tree_transactions = ttk.Treeview(self.tx_frame, show="headings")
        self.tree_transactions.pack(fill=tk.BOTH, expand=True)

        # State Tab
        self.state_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.state_frame, text="State")
        self.state_canvas = tk.Canvas(self.state_frame)
        self.scroll_y = tk.Scrollbar(self.state_frame, orient="vertical", command=self.state_canvas.yview)
        self.state_inner = ttk.Frame(self.state_canvas)

        self.state_inner.bind("<Configure>", lambda e: self.state_canvas.configure(scrollregion=self.state_canvas.bbox("all")))
        self.state_canvas.create_window((0, 0), window=self.state_inner, anchor="nw")
        self.state_canvas.configure(yscrollcommand=self.scroll_y.set)

        self.state_canvas.pack(side="left", fill="both", expand=True)
        self.scroll_y.pack(side="right", fill="y")

        self.update_treeview()
        self.root.mainloop()

    def update_treeview(self):
        self.root.after(1000, self.update_treeview)

        with self.lock:
            # Blocks
            while self.blocks:
                block = self.blocks.pop(0)
                block_hash = block.get("BlockHash")
                if not block_hash or block_hash in self.seen_hashes:
                    continue

                self.seen_hashes.add(block_hash)

                if not self.columns:
                    self.columns = list(block.keys())
                    self.tree_blocks["columns"] = self.columns
                    for col in self.columns:
                        self.tree_blocks.heading(col, text=col)
                        width = max(100, len(col) * 10)
                        self.tree_blocks.column(col, width=width, anchor="center")

                values = [block.get(col, "N/A") for col in self.columns]
                self.tree_blocks.insert("", "end", values=values)

            # Transactions
            while self.transactions:
                tx = self.transactions.pop(0)
                tx_id = tx.get("id")
                if not tx_id or tx_id in self.seen_tx_ids:
                    continue

                self.seen_tx_ids.add(tx_id)

                if not self.tx_columns:
                    self.tx_columns = list(tx.keys())
                    self.tree_transactions["columns"] = self.tx_columns
                    for col in self.tx_columns:
                        self.tree_transactions.heading(col, text=col)
                        width = max(100, len(col) * 10)
                        self.tree_transactions.column(col, width=width, anchor="center")

                values = [tx.get(col, "N/A") for col in self.tx_columns]
                self.tree_transactions.insert("", "end", values=values)

            # State
            if self.latest_state:
                self.render_state(self.latest_state)
                self.latest_state = None

    def send_blocks(self, block_list):
        with self.lock:
            for block_str in block_list:
                block = json.loads(block_str)
                block_hash = block.get("BlockHash")
                if block_hash and block_hash not in self.seen_hashes:
                    self.blocks.append(block)

    def send_transactions(self, tx_list):
        with self.lock:
            for tx in tx_list:
                tx_dict = self.transaction_to_dict(tx)
                tx_id = tx_dict.get("id")
                if tx_id and tx_id not in self.seen_tx_ids:
                    self.transactions.append(tx_dict)

    def display_state(self, state_obj):
        with self.lock:
            if state_obj.state_hash != self.last_state_hash:
                self.latest_state = state_obj
                self.last_state_hash = state_obj.state_hash


    def render_state(self, state_obj):
        for widget in self.state_inner.winfo_children():
            widget.destroy()

        row = 0

        for key, value in state_obj.__dict__.items():
            if key.startswith("_"):  # Skip private fields
                continue

            ttk.Label(self.state_inner, text=f"{key}:", font=("Arial", 10, "bold")).grid(row=row, column=0, sticky="w", padx=5, pady=5)
            row += 1

            if isinstance(value, list) and all(isinstance(item, dict) for item in value):
                # List of dictionaries
                if value:
                    headers = list(value[0].keys())
                    column_widths = {h: len(h) for h in headers}

                    # Compute widths, skip iterable fields for now
                    for item in value:
                        for h in headers:
                            content = str(item.get(h, ""))
                            if not isinstance(value[0].get(h), (list, dict)):
                                if isinstance(value[0].get(h), str) and len(value[0].get(h))<=15:
                                    column_widths[h] = max(column_widths[h], len(content))

                    # Only use scalar fields as table headers
                    scalar_headers = [h for h in headers if not isinstance(value[0].get(h), (list, dict))]

                    for col, header in enumerate(scalar_headers):
                        ttk.Label(self.state_inner, text=header, font=("Arial", 9, "bold"),
                                  width=column_widths[header] + 2).grid(row=row, column=col, sticky="w", padx=5)
                    row += 1

                    for item in value:
                        # Row with scalar values
                        for col, header in enumerate(scalar_headers):
                            val = str(item.get(header, ""))
                            ttk.Label(self.state_inner, text=val,
                                      width=column_widths[header] + 2).grid(row=row, column=col, sticky="w", padx=5)
                        row += 1

                        # Dedicated rows for iterable fields
                        # for key, val in item.items():
                        #     if isinstance(val, (list, dict)):
                        #         # if isinstance(value[0].get(h), str) and len(item.get(h)) > 10:
                        #         ttk.Label(self.state_inner, text=f"{key}:", font=("Arial", 9, "italic")).grid(
                        #             row=row, column=0, sticky="w", padx=20)

                        #         content_str = json.dumps(val, indent=2) if isinstance(val, dict) else str(val)
                        #         content_display = tk.Text(self.state_inner, height=4, width=100, wrap="word")
                        #         content_display.insert("1.0", content_str)
                        #         content_display.configure(state="disabled", background=self.root.cget("background"), relief="flat")
                        #         content_display.grid(row=row, column=1, columnspan=5, sticky="w", padx=5)

                        #         row += 1


            elif isinstance(value, dict):
                # Display dictionary as key-value pairs
                headers = list(value.keys())
                for col, header in enumerate(headers):
                    ttk.Label(self.state_inner, text=header, font=("Arial", 9, "bold")).grid(row=row, column=col, sticky="w", padx=5)

                row += 1
                for col, header in enumerate(headers):
                    ttk.Label(self.state_inner, text=str(value[header])).grid(row=row, column=col, sticky="w", padx=5)
                row += 1

            elif isinstance(value, list):
                # Display simple list horizontally
                for col, item in enumerate(value):
                    ttk.Label(self.state_inner, text=str(item)).grid(row=row, column=col, sticky="w", padx=5)
                row += 1

            else:
                # Single value
                ttk.Label(self.state_inner, text=str(value)).grid(row=row, column=1, sticky="w", padx=5)
                row += 1


    def transaction_to_dict(self, tx):
        return {
            "id": tx.id,
            "sender": tx.sender,
            "receiver": tx.receiver,
            "value": tx.value,
            "timestamp": tx.timestamp,
            "nonce": tx.nonce,
            "data": tx.data
        }

    def start(self):
        pass 

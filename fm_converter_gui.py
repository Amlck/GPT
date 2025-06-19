"""Tkinter GUI to run the CSV to FM text conversion provided by :mod:`fm_converter`."""

import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
import traceback

import fm_converter

class ConverterGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        root.title("FM Converter")
        # --- REMOVED: The fixed geometry call that was causing the issue ---
        # root.geometry("600x650")

        # --- Use .pack() for main containers for a stable vertical layout ---
        # ... (the rest of the layout code is correct and remains the same)

        # --- Task Selection ---
        task_frame = tk.LabelFrame(root, text="Task", padx=10, pady=5)
        task_frame.pack(fill="x", padx=10, pady=5)

        self.task_var = tk.StringVar(value="matched")
        self.task_var.trace_add("write", self._update_gui_state)

        tk.Radiobutton(task_frame, text="Convert Matched Cases", variable=self.task_var, value="matched").pack(anchor="w")
        tk.Radiobutton(task_frame, text="Generate New 'B' Class Cases", variable=self.task_var, value="unmatched").pack(anchor="w")
        tk.Radiobutton(task_frame, text="Generate Next Batch from Rejection File", variable=self.task_var, value="next_batch").pack(anchor="w")

        # --- File Inputs ---
        inputs_frame = tk.LabelFrame(root, text="File Inputs", padx=10, pady=5)
        inputs_frame.pack(fill="x", padx=10, pady=5)
        inputs_frame.columnconfigure(1, weight=1)

        self.segment_frame = tk.Frame(inputs_frame)
        self.segment_frame.grid(row=0, column=0, columnspan=3, sticky='w', pady=(0, 5))
        tk.Label(self.segment_frame, text="Use Case").pack(side="left", padx=(0, 10))
        self.segment_var = tk.StringVar(value="A")
        self.segment_var.trace_add("write", self._update_gui_state)
        tk.Radiobutton(self.segment_frame, text="A: New/Open Case", variable=self.segment_var, value="A").pack(side="left")
        tk.Radiobutton(self.segment_frame, text="B: Closed Case", variable=self.segment_var, value="B").pack(side="left")

        tk.Label(inputs_frame, text="整年度看診名單").grid(row=1, column=0, sticky="e", padx=(0, 10))
        self.long_var = tk.StringVar()
        tk.Entry(inputs_frame, textvariable=self.long_var).grid(row=1, column=1, sticky="ew")
        tk.Button(inputs_frame, text="Browse", command=self.browse_long).grid(row=1, column=2, padx=(5, 0))

        tk.Label(inputs_frame, text="健保署下載名單").grid(row=2, column=0, sticky="e", padx=(0, 10))
        self.short_var = tk.StringVar()
        tk.Entry(inputs_frame, textvariable=self.short_var).grid(row=2, column=1, sticky="ew")
        tk.Button(inputs_frame, text="Browse", command=self.browse_short).grid(row=2, column=2, padx=(5, 0))

        self.rejection_file_label = tk.Label(inputs_frame, text="健保署錯誤名單")
        self.rejection_file_label.grid(row=3, column=0, sticky="e", padx=(0, 10))
        self.rejection_var = tk.StringVar()
        self.rejection_entry = tk.Entry(inputs_frame, textvariable=self.rejection_var)
        self.rejection_entry.grid(row=3, column=1, sticky="ew")
        self.rejection_button = tk.Button(inputs_frame, text="Browse", command=self.browse_rejection)
        self.rejection_button.grid(row=3, column=2, padx=(5, 0))

        # --- Fixed Parameters ---
        params_frame = tk.LabelFrame(root, text="Parameters", padx=10, pady=5)
        params_frame.pack(fill="x", padx=10, pady=5)
        params_frame.columnconfigure(1, weight=1)

        tk.Label(params_frame, text="期別").grid(row=0, column=0, sticky="w")
        self.plan_var = tk.StringVar()
        tk.Entry(params_frame, textvariable=self.plan_var).grid(row=0, column=1, sticky="ew", pady=2)

        tk.Label(params_frame, text="健保署分區").grid(row=1, column=0, sticky="w")
        self.branch_var = tk.StringVar()
        tk.Entry(params_frame, textvariable=self.branch_var).grid(row=1, column=1, sticky="ew", pady=2)

        tk.Label(params_frame, text="院所代碼").grid(row=2, column=0, sticky="w")
        self.hosp_var = tk.StringVar()
        tk.Entry(params_frame, textvariable=self.hosp_var).grid(row=2, column=1, sticky="ew", pady=2)

        tk.Label(params_frame, text="醫師身分證字號").grid(row=3, column=0, sticky="w")
        self.prsn_var = tk.StringVar()
        tk.Entry(params_frame, textvariable=self.prsn_var).grid(row=3, column=1, sticky="ew", pady=2)

        tk.Label(params_frame, text="Upload Month (MM)").grid(row=4, column=0, sticky="w")
        self.month_var = tk.StringVar()
        tk.Entry(params_frame, textvariable=self.month_var).grid(row=4, column=1, sticky="ew", pady=2)

        tk.Label(params_frame, text="Case Start Date (YYYYMMDD)").grid(row=5, column=0, sticky="w")
        self.start_date_var = tk.StringVar()
        tk.Entry(params_frame, textvariable=self.start_date_var).grid(row=5, column=1, sticky="ew", pady=2)

        self.end_date_label = tk.Label(params_frame, text="Case End Date (YYYYMMDD)")
        self.end_date_label.grid(row=6, column=0, sticky="w")
        self.end_date_var = tk.StringVar()
        self.end_date_entry = tk.Entry(params_frame, textvariable=self.end_date_var)
        self.end_date_entry.grid(row=6, column=1, sticky="ew", pady=2)

        self.close_rsn_label = tk.Label(params_frame, text="Close Reason (1-3)")
        self.close_rsn_label.grid(row=7, column=0, sticky="w")
        self.close_rsn_var = tk.StringVar()
        self.close_rsn_entry = tk.Entry(params_frame, textvariable=self.close_rsn_var)
        self.close_rsn_entry.grid(row=7, column=1, sticky="ew", pady=2)

        tk.Label(params_frame, text="Start Sequence (NN)").grid(row=8, column=0, sticky="w")
        self.seq_var = tk.StringVar(value="1")
        tk.Entry(params_frame, textvariable=self.seq_var).grid(row=8, column=1, sticky="ew", pady=2)

        output_frame = tk.LabelFrame(root, text="Output", padx=10, pady=5)
        output_frame.pack(fill="x", padx=10, pady=5)
        output_frame.columnconfigure(1, weight=1)

        tk.Label(output_frame, text="Output Directory").grid(row=0, column=0, sticky="e", padx=(0,10))
        self.out_var = tk.StringVar(value="output")
        tk.Entry(output_frame, textvariable=self.out_var).grid(row=0, column=1, sticky="ew")
        tk.Button(output_frame, text="Browse", command=self.browse_outdir).grid(row=0, column=2, padx=(5,0))

        self.big5_var = tk.BooleanVar()
        tk.Checkbutton(output_frame, text="Big-5 Output", variable=self.big5_var).grid(row=1, column=1, sticky="w")

        action_frame = tk.Frame(root)
        action_frame.pack(fill="x", pady=10)
        tk.Button(action_frame, text="Convert", command=self.convert, font=("Helvetica", 12, "bold")).pack()

        self._update_gui_state()

        # --- ADDED: Set the minimum size AFTER all widgets are created ---
        # This ensures the window is at least big enough to show everything.
        root.update_idletasks() # Ensure all widgets are drawn before getting size
        root.minsize(root.winfo_reqwidth(), root.winfo_reqheight())

    # ... (the rest of the ConverterGUI class and the main() function are unchanged)
    # They are included here for completeness.
    def _update_gui_state(self, *args) -> None:
        """Enable or disable fields based on the selected task."""
        task = self.task_var.get()

        if task == "next_batch":
            self.rejection_file_label.grid()
            self.rejection_entry.grid()
            self.rejection_button.grid()
        else:
            self.rejection_file_label.grid_remove()
            self.rejection_entry.grid_remove()
            self.rejection_button.grid_remove()
            self.rejection_var.set("")

        if task in ["unmatched", "next_batch"]:
            self.segment_frame.grid_remove()
        else:
            self.segment_frame.grid()

        is_closed_case = self.segment_var.get() == "B" and task == "matched"
        close_case_state = tk.NORMAL if is_closed_case else tk.DISABLED

        for widget in [self.end_date_label, self.end_date_entry, self.close_rsn_label, self.close_rsn_entry]:
            widget.config(state=close_case_state)

        if not is_closed_case:
            self.end_date_var.set("")
            self.close_rsn_var.set("")

    def browse_long(self):
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("All", "*")])
        if path: self.long_var.set(path)

    def browse_short(self):
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("All", "*")])
        if path: self.short_var.set(path)

    def browse_rejection(self):
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("All", "*")])
        if path: self.rejection_var.set(path)

    def browse_outdir(self):
        path = filedialog.askdirectory()
        if path: self.out_var.set(path)

    def convert(self) -> None:
        try:
            task_mode = self.task_var.get()
            segment_type = "A" if task_mode in ["unmatched", "next_batch"] else self.segment_var.get()

            required_fields = {
                "整年度看診名單": self.long_var, "健保署下載名單": self.short_var,
                "期別": self.plan_var, "健保署分區": self.branch_var,
                "院所代碼": self.hosp_var, "醫師身分證字號": self.prsn_var,
                "Upload Month (MM)": self.month_var, "Case Start Date (YYYYMMDD)": self.start_date_var,
                "Start Sequence (NN)": self.seq_var
            }
            if task_mode == "next_batch":
                required_fields["健保署錯誤名單"] = self.rejection_var
            if task_mode == "matched" and segment_type == "B":
                required_fields["Case End Date"] = self.end_date_var
                required_fields["Close Reason"] = self.close_rsn_var

            for name, var in required_fields.items():
                if not var.get():
                    messagebox.showerror("Input Error", f"The field '{name}' cannot be empty.")
                    return

            fixed = {
                "PLAN_NO": self.plan_var.get().zfill(2), "BRANCH_CODE": self.branch_var.get(),
                "HOSP_ID": self.hosp_var.get().zfill(10), "PRSN_ID": self.prsn_var.get().zfill(10),
            }

            rejection_path = Path(self.rejection_var.get()) if self.rejection_var.get() else None

            written_files = fm_converter.convert(
                long_path=Path(self.long_var.get()), short_path=Path(self.short_var.get()),
                fixed=fixed, upload_month=self.month_var.get(),
                start_date=self.start_date_var.get(), end_date=self.end_date_var.get(),
                segment_type=segment_type, close_reason=self.close_rsn_var.get(),
                seq_start=int(self.seq_var.get() or 1),
                out_encoding=fm_converter.BIG5 if self.big5_var.get() else fm_converter.ENCODING,
                outdir=Path(self.out_var.get()), mode=task_mode,
                rejection_path=rejection_path
            )

            if written_files:
                file_names = "\n".join([path.name for path in written_files])
                messagebox.showinfo("Success", f"Successfully created {len(written_files)} file(s) in the '{self.out_var.get()}' folder:\n\n{file_names}")
            else:
                messagebox.showwarning("Warning", "Conversion completed, but no output files were generated.")
        except Exception as exc:
            traceback.print_exc()
            messagebox.showerror("Error", f"An unexpected error occurred:\n\n{exc}\n\nCheck the console for a detailed traceback.")

def main() -> None:
    root = tk.Tk()
    ConverterGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
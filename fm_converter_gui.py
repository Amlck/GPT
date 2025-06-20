"""Tkinter GUI to run the CSV to FM text conversion provided by :mod:`fm_converter`."""

import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
import traceback

import fm_converter

class ConverterGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        root.title("FM Converter v2.5.1")

        # --- Task Selection ---
        task_frame = tk.LabelFrame(root, text="Task", padx=10, pady=5)
        task_frame.pack(fill="x", padx=10, pady=5)

        self.task_var = tk.StringVar(value="matched")
        self.task_var.trace_add("write", self._update_gui_state)

        tk.Radiobutton(task_frame, text="Convert Matched Cases (健保署名單)", variable=self.task_var, value="matched").pack(anchor="w")
        tk.Radiobutton(task_frame, text="Generate First Batch of New 'B' Class Cases (首次自選)", variable=self.task_var, value="unmatched").pack(anchor="w")
        tk.Radiobutton(task_frame, text="Refine a Submitted Batch with a Rejection File (根據錯誤名單修正)", variable=self.task_var, value="refine").pack(anchor="w")

        # --- File Inputs ---
        inputs_frame = tk.LabelFrame(root, text="File Inputs", padx=10, pady=5)
        inputs_frame.pack(fill="x", padx=10, pady=5)
        inputs_frame.columnconfigure(1, weight=1)

        self.segment_frame = tk.Frame(inputs_frame)
        self.segment_frame.grid(row=0, column=0, columnspan=3, sticky='w', pady=(0, 5))
        tk.Label(self.segment_frame, text="Use Case").pack(side="left", padx=(0, 10))
        self.segment_var = tk.StringVar(value="A"); self.segment_var.trace_add("write", self._update_gui_state)
        tk.Radiobutton(self.segment_frame, text="A: New/Open Case", variable=self.segment_var, value="A").pack(side="left")
        tk.Radiobutton(self.segment_frame, text="B: Closed Case", variable=self.segment_var, value="B").pack(side="left")

        tk.Label(inputs_frame, text="整年度看診名單").grid(row=1, column=0, sticky="e", padx=(0, 10)); self.long_var = tk.StringVar(); tk.Entry(inputs_frame, textvariable=self.long_var).grid(row=1, column=1, sticky="ew"); tk.Button(inputs_frame, text="Browse", command=self.browse_long).grid(row=1, column=2, padx=(5, 0))
        tk.Label(inputs_frame, text="健保署下載名單").grid(row=2, column=0, sticky="e", padx=(0, 10)); self.short_var = tk.StringVar(); tk.Entry(inputs_frame, textvariable=self.short_var).grid(row=2, column=1, sticky="ew"); tk.Button(inputs_frame, text="Browse", command=self.browse_short).grid(row=2, column=2, padx=(5, 0))

        self.submitted_file_label = tk.Label(inputs_frame, text="Submitted File (前次上傳檔案)")
        self.submitted_file_label.grid(row=3, column=0, sticky="e", padx=(0, 10)); self.submitted_var = tk.StringVar(); self.submitted_entry = tk.Entry(inputs_frame, textvariable=self.submitted_var); self.submitted_entry.grid(row=3, column=1, sticky="ew"); self.submitted_button = tk.Button(inputs_frame, text="Browse", command=self.browse_submitted); self.submitted_button.grid(row=3, column=2, padx=(5,0))

        self.rejection_file_label = tk.Label(inputs_frame, text="Rejection File (健保署錯誤名單)")
        self.rejection_file_label.grid(row=4, column=0, sticky="e", padx=(0, 10)); self.rejection_var = tk.StringVar(); self.rejection_entry = tk.Entry(inputs_frame, textvariable=self.rejection_var); self.rejection_entry.grid(row=4, column=1, sticky="ew"); self.rejection_button = tk.Button(inputs_frame, text="Browse", command=self.browse_rejection); self.rejection_button.grid(row=4, column=2, padx=(5,0))

        params_frame = tk.LabelFrame(root, text="Parameters", padx=10, pady=5)
        params_frame.pack(fill="x", padx=10, pady=5)
        params_frame.columnconfigure(1, weight=1)

        tk.Label(params_frame, text="期別").grid(row=0, column=0, sticky="w", pady=2); self.plan_var = tk.StringVar(); tk.Entry(params_frame, textvariable=self.plan_var).grid(row=0, column=1, sticky="ew")
        tk.Label(params_frame, text="健保署分區").grid(row=1, column=0, sticky="w", pady=2); self.branch_var = tk.StringVar(); tk.Entry(params_frame, textvariable=self.branch_var).grid(row=1, column=1, sticky="ew")
        tk.Label(params_frame, text="院所代碼").grid(row=2, column=0, sticky="w", pady=2); self.hosp_var = tk.StringVar(); tk.Entry(params_frame, textvariable=self.hosp_var).grid(row=2, column=1, sticky="ew")
        tk.Label(params_frame, text="醫師身分證字號").grid(row=3, column=0, sticky="w", pady=2); self.prsn_var = tk.StringVar(); tk.Entry(params_frame, textvariable=self.prsn_var).grid(row=3, column=1, sticky="ew")
        tk.Label(params_frame, text="Upload Month (MM)").grid(row=4, column=0, sticky="w", pady=2); self.month_var = tk.StringVar(); tk.Entry(params_frame, textvariable=self.month_var).grid(row=4, column=1, sticky="ew")
        tk.Label(params_frame, text="Case Start Date (YYYYMMDD)").grid(row=5, column=0, sticky="w", pady=2); self.start_date_var = tk.StringVar(); tk.Entry(params_frame, textvariable=self.start_date_var).grid(row=5, column=1, sticky="ew")
        self.end_date_label = tk.Label(params_frame, text="Case End Date (YYYYMMDD)"); self.end_date_label.grid(row=6, column=0, sticky="w", pady=2); self.end_date_var = tk.StringVar(); self.end_date_entry = tk.Entry(params_frame, textvariable=self.end_date_var); self.end_date_entry.grid(row=6, column=1, sticky="ew")
        self.close_rsn_label = tk.Label(params_frame, text="Close Reason (1-3)"); self.close_rsn_label.grid(row=7, column=0, sticky="w", pady=2); self.close_rsn_var = tk.StringVar(); self.close_rsn_entry = tk.Entry(params_frame, textvariable=self.close_rsn_var); self.close_rsn_entry.grid(row=7, column=1, sticky="ew")
        tk.Label(params_frame, text="Start Sequence (NN)").grid(row=8, column=0, sticky="w", pady=2); self.seq_var = tk.StringVar(value="1"); tk.Entry(params_frame, textvariable=self.seq_var).grid(row=8, column=1, sticky="ew")

        output_frame = tk.LabelFrame(root, text="Output", padx=10, pady=5)
        output_frame.pack(fill="x", padx=10, pady=5); output_frame.columnconfigure(1, weight=1)
        tk.Label(output_frame, text="Output Directory").grid(row=0, column=0, sticky="e", padx=(0,10)); self.out_var=tk.StringVar(value="output"); tk.Entry(output_frame, textvariable=self.out_var).grid(row=0, column=1, sticky="ew"); tk.Button(output_frame, text="Browse", command=self.browse_outdir).grid(row=0, column=2, padx=(5,0))
        self.big5_var = tk.BooleanVar(); tk.Checkbutton(output_frame, text="Big-5 Output", variable=self.big5_var).grid(row=1, column=1, sticky="w")

        action_frame = tk.Frame(root); action_frame.pack(fill="x", pady=10); tk.Button(action_frame, text="Convert", command=self.convert, font=("Helvetica", 12, "bold")).pack()

        self._update_gui_state()
        root.update_idletasks(); root.minsize(root.winfo_reqwidth(), root.winfo_reqheight())

    def _update_gui_state(self, *args):
        task = self.task_var.get()

        is_refine_mode = task == "refine"
        for widget in [self.submitted_file_label, self.submitted_entry, self.submitted_button, self.rejection_file_label, self.rejection_entry, self.rejection_button]:
            if is_refine_mode: widget.grid()
            else: widget.grid_remove()
        if not is_refine_mode: self.submitted_var.set(""); self.rejection_var.set("")

        is_matched_mode = task == "matched"
        if is_matched_mode: self.segment_frame.grid()
        else: self.segment_frame.grid_remove()

        is_closed_case = self.segment_var.get() == "B" and is_matched_mode
        state = tk.NORMAL if is_closed_case else tk.DISABLED
        for widget in [self.end_date_label, self.end_date_entry, self.close_rsn_label, self.close_rsn_entry]: widget.config(state=state)
        if not is_closed_case: self.end_date_var.set(""); self.close_rsn_var.set("")

    def browse_long(self): path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("All", "*")]); self.long_var.set(path) if path else None
    def browse_short(self): path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("All", "*")]); self.short_var.set(path) if path else None
    def browse_rejection(self): path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("All", "*")]); self.rejection_var.set(path) if path else None
    def browse_submitted(self): path = filedialog.askopenfilename(filetypes=[("Text Files", "*.txt"), ("All", "*")]); self.submitted_var.set(path) if path else None
    def browse_outdir(self): path = filedialog.askdirectory(); self.out_var.set(path) if path else None

    def convert(self) -> None:
        try:
            task_mode = self.task_var.get()
            segment_type = "A" if task_mode in ["unmatched", "refine"] else self.segment_var.get()

            required = {"整年度看診名單": self.long_var, "健保署下載名單": self.short_var, "期別": self.plan_var, "健保署分區": self.branch_var, "院所代碼": self.hosp_var, "醫師身分證字號": self.prsn_var, "Upload Month (MM)": self.month_var, "Case Start Date": self.start_date_var, "Start Sequence": self.seq_var}
            if task_mode == "refine": required["Submitted File"] = self.submitted_var; required["Rejection File"] = self.rejection_var
            if task_mode == "matched" and segment_type == "B": required["Case End Date"] = self.end_date_var; required["Close Reason"] = self.close_rsn_var

            for name, var in required.items():
                if not var.get(): messagebox.showerror("Input Error", f"The field '{name}' cannot be empty."); return

            fixed = {"PLAN_NO": self.plan_var.get().zfill(2), "BRANCH_CODE": self.branch_var.get(), "HOSP_ID": self.hosp_var.get().zfill(10), "PRSN_ID": self.prsn_var.get().zfill(10)}

            written_files = fm_converter.convert(
                long_path=Path(self.long_var.get()), short_path=Path(self.short_var.get()),
                fixed=fixed, upload_month=self.month_var.get(), start_date=self.start_date_var.get(), end_date=self.end_date_var.get(),
                segment_type=segment_type, close_reason=self.close_rsn_var.get(), seq_start=int(self.seq_var.get() or 1),
                out_encoding=fm_converter.BIG5 if self.big5_var.get() else fm_converter.ENCODING,
                outdir=Path(self.out_var.get()), mode=task_mode,
                rejection_path=Path(self.rejection_var.get()) if self.rejection_var.get() else None,
                submitted_path=Path(self.submitted_var.get()) if self.submitted_var.get() else None
            )

            if written_files: messagebox.showinfo("Success", f"Successfully created {len(written_files)} file(s):\n\n" + "\n".join([p.name for p in written_files]))
            else: messagebox.showwarning("Warning", "Conversion completed, but no output files were generated.")
        except Exception as exc: traceback.print_exc(); messagebox.showerror("Error", f"An unexpected error occurred:\n\n{exc}\n\nCheck console for details.")

def main() -> None:
    root = tk.Tk()
    ConverterGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
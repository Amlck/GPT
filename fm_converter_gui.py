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

        # --- ADDED: Use Case (Segment) Selection ---
        tk.Label(root, text="Use Case").grid(row=0, column=0, sticky="e", padx=5, pady=2)
        self.segment_var = tk.StringVar(value="A")
        self.segment_var.trace_add("write", self._update_gui_state)
        tk.Radiobutton(root, text="A: New/Open Case", variable=self.segment_var, value="A").grid(row=0, column=1, sticky="w")
        tk.Radiobutton(root, text="B: Closed Case", variable=self.segment_var, value="B").grid(row=0, column=2, sticky="w")

        # --- MODIFIED: Row numbers shifted down ---
        # Long CSV
        tk.Label(root, text="Long CSV").grid(row=1, column=0, sticky="e", padx=5, pady=2)
        self.long_var = tk.StringVar()
        tk.Entry(root, textvariable=self.long_var, width=50).grid(row=1, column=1, columnspan=2)
        tk.Button(root, text="Browse", command=self.browse_long).grid(row=1, column=3)

        # Short CSV
        tk.Label(root, text="Short CSV").grid(row=2, column=0, sticky="e", padx=5, pady=2)
        self.short_var = tk.StringVar()
        tk.Entry(root, textvariable=self.short_var, width=50).grid(row=2, column=1, columnspan=2)
        tk.Button(root, text="Browse", command=self.browse_short).grid(row=2, column=3)

        # PLAN_NO
        tk.Label(root, text="PLAN_NO").grid(row=3, column=0, sticky="e", padx=5, pady=2)
        self.plan_var = tk.StringVar()
        tk.Entry(root, textvariable=self.plan_var).grid(row=3, column=1, sticky="w")

        # BRANCH_CODE
        tk.Label(root, text="BRANCH_CODE").grid(row=4, column=0, sticky="e", padx=5, pady=2)
        self.branch_var = tk.StringVar()
        tk.Entry(root, textvariable=self.branch_var).grid(row=4, column=1, sticky="w")

        # HOSP_ID
        tk.Label(root, text="HOSP_ID").grid(row=5, column=0, sticky="e", padx=5, pady=2)
        self.hosp_var = tk.StringVar()
        tk.Entry(root, textvariable=self.hosp_var).grid(row=5, column=1, sticky="w")

        # PRSN_ID
        tk.Label(root, text="PRSN_ID").grid(row=6, column=0, sticky="e", padx=5, pady=2)
        self.prsn_var = tk.StringVar()
        tk.Entry(root, textvariable=self.prsn_var).grid(row=6, column=1, sticky="w")

        # Upload Month
        tk.Label(root, text="Upload Month (MM)").grid(row=7, column=0, sticky="e", padx=5, pady=2)
        self.month_var = tk.StringVar()
        tk.Entry(root, textvariable=self.month_var).grid(row=7, column=1, sticky="w")

        # Case Start Date Input
        tk.Label(root, text="Case Start Date (YYYYMMDD)").grid(row=8, column=0, sticky="e", padx=5, pady=2)
        self.start_date_var = tk.StringVar()
        tk.Entry(root, textvariable=self.start_date_var).grid(row=8, column=1, sticky="w")

        # Case End Date Input
        self.end_date_label = tk.Label(root, text="Case End Date (YYYYMMDD)")
        self.end_date_label.grid(row=9, column=0, sticky="e", padx=5, pady=2)
        self.end_date_var = tk.StringVar()
        self.end_date_entry = tk.Entry(root, textvariable=self.end_date_var)
        self.end_date_entry.grid(row=9, column=1, sticky="w")

        # --- ADDED: Close Reason Input ---
        self.close_rsn_label = tk.Label(root, text="Close Reason (1-3)")
        self.close_rsn_label.grid(row=10, column=0, sticky="e", padx=5, pady=2)
        self.close_rsn_var = tk.StringVar()
        self.close_rsn_entry = tk.Entry(root, textvariable=self.close_rsn_var)
        self.close_rsn_entry.grid(row=10, column=1, sticky="w")

        # --- MODIFIED: Row numbers shifted down ---
        # Sequence start
        tk.Label(root, text="Start Sequence (NN)").grid(row=11, column=0, sticky="e", padx=5, pady=2)
        self.seq_var = tk.StringVar(value="1")
        tk.Entry(root, textvariable=self.seq_var).grid(row=11, column=1, sticky="w")

        # Output directory
        tk.Label(root, text="Output Directory").grid(row=12, column=0, sticky="e", padx=5, pady=2)
        self.out_var = tk.StringVar(value="output")
        tk.Entry(root, textvariable=self.out_var, width=50).grid(row=12, column=1, columnspan=2)
        tk.Button(root, text="Browse", command=self.browse_outdir).grid(row=12, column=3)

        # Big-5 checkbox
        self.big5_var = tk.BooleanVar()
        tk.Checkbutton(root, text="Big-5 Output", variable=self.big5_var).grid(row=13, column=1, sticky="w")

        # Convert button
        tk.Button(root, text="Convert", command=self.convert).grid(row=14, column=1, pady=10)

        # --- ADDED: Initial GUI state setup ---
        self._update_gui_state()

    # --- ADDED: Function to dynamically enable/disable fields ---
    def _update_gui_state(self, *args) -> None:
        """Enable or disable fields based on the selected segment."""
        is_closed_case = self.segment_var.get() == "B"
        new_state = tk.NORMAL if is_closed_case else tk.DISABLED

        self.end_date_entry.config(state=new_state)
        self.end_date_label.config(state=new_state)
        self.close_rsn_entry.config(state=new_state)
        self.close_rsn_label.config(state=new_state)

        if not is_closed_case:
            self.end_date_var.set("")
            self.close_rsn_var.set("")

    def browse_long(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("All", "*")])
        if path:
            self.long_var.set(path)

    def browse_short(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("All", "*")])
        if path:
            self.short_var.set(path)

    def browse_outdir(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.out_var.set(path)

    def convert(self) -> None:
        try:
            # --- MODIFIED: Validation is now dynamic based on segment ---
            segment_type = self.segment_var.get()

            required_fields = {
                "Long CSV": self.long_var, "Short CSV": self.short_var,
                "PLAN_NO": self.plan_var, "BRANCH_CODE": self.branch_var,
                "HOSP_ID": self.hosp_var, "PRSN_ID": self.prsn_var,
                "Upload Month (MM)": self.month_var, "Case Start Date": self.start_date_var,
                "Start Sequence (NN)": self.seq_var
            }

            # If it's a closed case, end date and reason are also required
            if segment_type == "B":
                required_fields["Case End Date"] = self.end_date_var
                required_fields["Close Reason"] = self.close_rsn_var

            for name, var in required_fields.items():
                if not var.get():
                    messagebox.showerror("Input Error", f"The field '{name}' cannot be empty.")
                    return

            # Main conversion logic
            fixed = {
                "PLAN_NO": self.plan_var.get().zfill(2),
                "BRANCH_CODE": self.branch_var.get(),
                "HOSP_ID": self.hosp_var.get().zfill(10),
                "PRSN_ID": self.prsn_var.get().zfill(10),
            }

            # --- MODIFIED: Pass new segment, close date, and reason to backend ---
            written_files = fm_converter.convert(
                long_path=Path(self.long_var.get()),
                short_path=Path(self.short_var.get()),
                fixed=fixed,
                upload_month=self.month_var.get(),
                start_date=self.start_date_var.get(),
                end_date=self.end_date_var.get(), # Will be "" for segment A
                segment_type=segment_type,
                close_reason=self.close_rsn_var.get(), # Will be "" for segment A
                seq_start=int(self.seq_var.get() or 1),
                out_encoding=fm_converter.BIG5 if self.big5_var.get() else fm_converter.ENCODING,
                outdir=Path(self.out_var.get()),
            )

            if written_files:
                file_names = "\n".join([path.name for path in written_files])
                success_message = f"Successfully created {len(written_files)} file(s) in the '{self.out_var.get()}' folder:\n\n{file_names}"
                messagebox.showinfo("Success", success_message)
            else:
                messagebox.showwarning("Warning", "Conversion completed, but no output files were generated. This may be due to no matching records between the two CSV files.")

        except Exception as exc:
            print("\n--- ERROR TRACEBACK ---")
            traceback.print_exc()
            print("------------------------\n")
            messagebox.showerror("Error", f"An unexpected error occurred:\n\n{exc}\n\nCheck the console for a detailed traceback.")

def main() -> None:
    root = tk.Tk()
    ConverterGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
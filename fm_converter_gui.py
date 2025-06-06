import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path

import fm_converter

class ConverterGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        root.title("FM Converter")

        # Long CSV
        tk.Label(root, text="Long CSV").grid(row=0, column=0, sticky="e")
        self.long_var = tk.StringVar()
        tk.Entry(root, textvariable=self.long_var, width=40).grid(row=0, column=1)
        tk.Button(root, text="Browse", command=self.browse_long).grid(row=0, column=2)

        # Short CSV
        tk.Label(root, text="Short CSV").grid(row=1, column=0, sticky="e")
        self.short_var = tk.StringVar()
        tk.Entry(root, textvariable=self.short_var, width=40).grid(row=1, column=1)
        tk.Button(root, text="Browse", command=self.browse_short).grid(row=1, column=2)

        # PLAN_NO
        tk.Label(root, text="PLAN_NO").grid(row=2, column=0, sticky="e")
        self.plan_var = tk.StringVar()
        tk.Entry(root, textvariable=self.plan_var).grid(row=2, column=1)

        # BRANCH_CODE
        tk.Label(root, text="BRANCH_CODE").grid(row=3, column=0, sticky="e")
        self.branch_var = tk.StringVar()
        tk.Entry(root, textvariable=self.branch_var).grid(row=3, column=1)

        # HOSP_ID
        tk.Label(root, text="HOSP_ID").grid(row=4, column=0, sticky="e")
        self.hosp_var = tk.StringVar()
        tk.Entry(root, textvariable=self.hosp_var).grid(row=4, column=1)

        # PRSN_ID
        tk.Label(root, text="PRSN_ID").grid(row=5, column=0, sticky="e")
        self.prsn_var = tk.StringVar()
        tk.Entry(root, textvariable=self.prsn_var).grid(row=5, column=1)

        # Upload Month
        tk.Label(root, text="Upload Month (MM)").grid(row=6, column=0, sticky="e")
        self.month_var = tk.StringVar()
        tk.Entry(root, textvariable=self.month_var).grid(row=6, column=1)

        # Sequence start
        tk.Label(root, text="Start Sequence (NN)").grid(row=7, column=0, sticky="e")
        self.seq_var = tk.StringVar(value="1")
        tk.Entry(root, textvariable=self.seq_var).grid(row=7, column=1)

        # Output directory
        tk.Label(root, text="Output Directory").grid(row=8, column=0, sticky="e")
        self.out_var = tk.StringVar(value="output")
        tk.Entry(root, textvariable=self.out_var, width=40).grid(row=8, column=1)
        tk.Button(root, text="Browse", command=self.browse_outdir).grid(row=8, column=2)

        # UTF-8 checkbox
        self.utf8_var = tk.BooleanVar()
        tk.Checkbutton(root, text="UTF-8 Output", variable=self.utf8_var).grid(row=9, column=1, sticky="w")

        # Convert button
        tk.Button(root, text="Convert", command=self.convert).grid(row=10, column=1, pady=10)

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
            fixed = {
                "PLAN_NO": self.plan_var.get().zfill(2),
                "BRANCH_CODE": self.branch_var.get(),
                "HOSP_ID": self.hosp_var.get().zfill(10),
                "PRSN_ID": self.prsn_var.get().zfill(10),
            }
            fm_converter.convert(
                Path(self.long_var.get()),
                Path(self.short_var.get()),
                fixed,
                self.month_var.get(),
                int(self.seq_var.get() or 1),
                out_encoding="utf-8" if self.utf8_var.get() else fm_converter.BIG5,
                outdir=Path(self.out_var.get()),
            )
            messagebox.showinfo("Success", "Conversion completed")
        except Exception as exc:
            messagebox.showerror("Error", str(exc))


def main() -> None:
    root = tk.Tk()
    ConverterGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()

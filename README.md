# NHI FM Converter

This repository contains tools for converting the Taiwan NHI CSV exports into the fixed-width **FM.txt** format described in `QM_UploadFormatFM.pdf`.

Two interfaces are provided:

* **Command line** – `fm_converter.py`
* **Graphical user interface** – `fm_converter_gui.py`

## Requirements

* Python 3.10+
* `pandas`
* `chardet`
* `tkinter` (bundled with Python)

Install dependencies with:

```bash
pip install pandas chardet
```

## Command line usage

```bash
python fm_converter.py --long long.csv --short short.csv [--big5]
```

The tool auto-detects the encoding of each CSV file, so they do not need to be
strictly UTF‑8. The output `FM.txt` will be UTF‑8 by default; pass `--big5` to
generate Big‑5 encoded output instead.

The script will prompt for the constant parameters (PLAN_NO, BRANCH_CODE,
etc.) and create one or more `FM.txt` files in the `output/` directory by
default.

## GUI usage

Launch the graphical tool with:

```bash
python fm_converter_gui.py
```

The GUI allows you to select the input files and output directory using file
dialogs. Enable the **Big‑5 Output** checkbox if you need legacy encoding.

### Building an executable

To package the GUI as a standalone executable you can use
[PyInstaller](https://pyinstaller.org/):

```bash
pip install pyinstaller
pyinstaller --onefile --noconsole fm_converter_gui.py
```

The resulting binary will be located in the `dist/` folder.

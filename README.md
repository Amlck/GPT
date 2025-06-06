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
python fm_converter.py --long long.csv --short short.csv
```

Use the `--utf8` flag if your text editor cannot display Big‑5 encoded
Chinese characters.

The script will prompt for the constant parameters (PLAN_NO, BRANCH_CODE,
etc.) and create one or more `FM.txt` files in the `output/` directory by
default.

## GUI usage

Launch the graphical tool with:

```bash
python fm_converter_gui.py
```

The GUI allows you to select the input files and output directory using file
dialogs.

### Building an executable

To package the GUI as a standalone executable you can use
[PyInstaller](https://pyinstaller.org/):

```bash
pip install pyinstaller
pyinstaller --onefile --noconsole fm_converter_gui.py
```

The resulting binary will be located in the `dist/` folder.

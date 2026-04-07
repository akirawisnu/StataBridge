#!/usr/bin/env python3
"""
StataBridge - Universal Stata Data Converter
Supports Stata versions 7–19 (dta formats 108–120)
Also handles CSV, XLSX/XLS, DBF, RData
@akirawisnu - 2026 v1.0.0
"""

import os
import sys
import re
import glob
import traceback
import datetime
from pathlib import Path
from typing import Optional

# ─────────────────────────────────────────────
#  LANGUAGE STRINGS
# ─────────────────────────────────────────────
LANGUAGES = {
    "English":    "en",
    "Indonesian": "id",
    "Vietnamese": "vi",
    "German":     "de",
    "French":     "fr",
    "Spanish":    "es",
    "Italian":    "it",
    "Portuguese": "pt",
    "Japanese":   "ja",
    "Chinese":    "zh",
}

T = {
    "en": {
        "welcome":         "Welcome to StataBridge — Universal Stata Data Converter",
        "select_lang":     "Select language / Pilih bahasa / Sprache wählen:",
        "main_menu":       "Main Menu",
        "opt_single":      "Single file conversion",
        "opt_multiple":    "Multiple files conversion (select files)",
        "opt_batch":       "Batch convert entire folder",
        "opt_settings":    "Settings",
        "opt_quit":        "Quit",
        "choose":          "Your choice",
        "input_file":      "Input file path",
        "input_dir":       "Input folder path",
        "output_dir":      "Output folder path (leave blank = same as input)",
        "output_format":   "Output format",
        "stata_version":   "Target Stata version",
        "converting":      "Converting",
        "done":            "Done!",
        "error":           "Error",
        "success":         "Success",
        "files_found":     "files found",
        "no_files":        "No supported files found.",
        "overwrite":       "Overwrite existing output files?",
        "yes":             "Yes",
        "no":              "No",
        "back":            "Back",
        "strl_warn":       "strL columns detected — will be truncated to str2045 for old Stata format",
        "label_warn":      "Variable labels truncated to fit target format",
        "summary":         "Conversion Summary",
        "converted":       "Converted",
        "skipped":         "Skipped",
        "failed":          "Failed",
        "press_enter":     "Press Enter to continue...",
        "select_files":    "Enter file paths (comma-separated or glob, e.g. *.dta)",
        "incl_subdir":     "Include subdirectories?",
        "filter_ext":      "Filter by extension (leave blank for all supported)",
        "settings_title":  "Settings",
        "cur_lang":        "Current language",
        "change_lang":     "Change language",
        "default_out_fmt": "Default output format",
        "default_stata_v": "Default Stata version",
        "version_table":   "Stata Version Reference",
        "file_num":        "File",
        "of":              "of",
        "browse_hint":     "Tip: you can drag & drop files/folders into the terminal",
    },
    "id": {
        "welcome":         "Selamat datang di StataBridge — Konverter Data Stata Universal",
        "select_lang":     "Pilih bahasa:",
        "main_menu":       "Menu Utama",
        "opt_single":      "Konversi satu file",
        "opt_multiple":    "Konversi beberapa file (pilih file)",
        "opt_batch":       "Konversi batch seluruh folder",
        "opt_settings":    "Pengaturan",
        "opt_quit":        "Keluar",
        "choose":          "Pilihan Anda",
        "input_file":      "Jalur file masukan",
        "input_dir":       "Jalur folder masukan",
        "output_dir":      "Jalur folder keluaran (kosongkan = sama dengan masukan)",
        "output_format":   "Format keluaran",
        "stata_version":   "Versi Stata target",
        "converting":      "Mengonversi",
        "done":            "Selesai!",
        "error":           "Kesalahan",
        "success":         "Berhasil",
        "files_found":     "file ditemukan",
        "no_files":        "Tidak ada file yang didukung ditemukan.",
        "overwrite":       "Timpa file keluaran yang sudah ada?",
        "yes":             "Ya",
        "no":              "Tidak",
        "back":            "Kembali",
        "strl_warn":       "Kolom strL terdeteksi — akan dipotong menjadi str2045 untuk format Stata lama",
        "label_warn":      "Label variabel dipotong agar sesuai format target",
        "summary":         "Ringkasan Konversi",
        "converted":       "Dikonversi",
        "skipped":         "Dilewati",
        "failed":          "Gagal",
        "press_enter":     "Tekan Enter untuk melanjutkan...",
        "select_files":    "Masukkan jalur file (pisahkan koma atau glob, mis. *.dta)",
        "incl_subdir":     "Sertakan subfolder?",
        "filter_ext":      "Filter berdasarkan ekstensi (kosongkan untuk semua yang didukung)",
        "settings_title":  "Pengaturan",
        "cur_lang":        "Bahasa saat ini",
        "change_lang":     "Ganti bahasa",
        "default_out_fmt": "Format keluaran default",
        "default_stata_v": "Versi Stata default",
        "version_table":   "Referensi Versi Stata",
        "file_num":        "File",
        "of":              "dari",
        "browse_hint":     "Tips: Anda bisa seret & lepas file/folder ke terminal",
    },
    "vi": {
        "welcome":         "Chào mừng đến StataBridge — Bộ chuyển đổi dữ liệu Stata toàn năng",
        "select_lang":     "Chọn ngôn ngữ:",
        "main_menu":       "Menu chính",
        "opt_single":      "Chuyển đổi một file",
        "opt_multiple":    "Chuyển đổi nhiều file (chọn file)",
        "opt_batch":       "Chuyển đổi hàng loạt toàn bộ thư mục",
        "opt_settings":    "Cài đặt",
        "opt_quit":        "Thoát",
        "choose":          "Lựa chọn của bạn",
        "input_file":      "Đường dẫn file đầu vào",
        "input_dir":       "Đường dẫn thư mục đầu vào",
        "output_dir":      "Đường dẫn thư mục đầu ra (để trống = giống đầu vào)",
        "output_format":   "Định dạng đầu ra",
        "stata_version":   "Phiên bản Stata đích",
        "converting":      "Đang chuyển đổi",
        "done":            "Xong!",
        "error":           "Lỗi",
        "success":         "Thành công",
        "files_found":     "file tìm thấy",
        "no_files":        "Không tìm thấy file nào được hỗ trợ.",
        "overwrite":       "Ghi đè file đầu ra đã tồn tại?",
        "yes":             "Có",
        "no":              "Không",
        "back":            "Quay lại",
        "strl_warn":       "Phát hiện cột strL — sẽ được cắt ngắn thành str2045 cho định dạng Stata cũ",
        "label_warn":      "Nhãn biến bị cắt ngắn để phù hợp với định dạng đích",
        "summary":         "Tóm tắt chuyển đổi",
        "converted":       "Đã chuyển đổi",
        "skipped":         "Đã bỏ qua",
        "failed":          "Thất bại",
        "press_enter":     "Nhấn Enter để tiếp tục...",
        "select_files":    "Nhập đường dẫn file (phân cách bằng dấu phẩy hoặc glob, vd. *.dta)",
        "incl_subdir":     "Bao gồm thư mục con?",
        "filter_ext":      "Lọc theo phần mở rộng (để trống cho tất cả được hỗ trợ)",
        "settings_title":  "Cài đặt",
        "cur_lang":        "Ngôn ngữ hiện tại",
        "change_lang":     "Thay đổi ngôn ngữ",
        "default_out_fmt": "Định dạng đầu ra mặc định",
        "default_stata_v": "Phiên bản Stata mặc định",
        "version_table":   "Tham chiếu phiên bản Stata",
        "file_num":        "File",
        "of":              "của",
        "browse_hint":     "Mẹo: bạn có thể kéo & thả file/thư mục vào terminal",
    },
    "de": {
        "welcome":         "Willkommen bei StataBridge — Universeller Stata-Datenkonverter",
        "select_lang":     "Sprache wählen:",
        "main_menu":       "Hauptmenü",
        "opt_single":      "Einzelne Datei konvertieren",
        "opt_multiple":    "Mehrere Dateien konvertieren (Dateien auswählen)",
        "opt_batch":       "Gesamten Ordner batch-konvertieren",
        "opt_settings":    "Einstellungen",
        "opt_quit":        "Beenden",
        "choose":          "Ihre Wahl",
        "input_file":      "Eingabedateipfad",
        "input_dir":       "Eingabeordnerpfad",
        "output_dir":      "Ausgabeordnerpfad (leer lassen = gleich wie Eingabe)",
        "output_format":   "Ausgabeformat",
        "stata_version":   "Ziel-Stata-Version",
        "converting":      "Konvertiere",
        "done":            "Fertig!",
        "error":           "Fehler",
        "success":         "Erfolg",
        "files_found":     "Dateien gefunden",
        "no_files":        "Keine unterstützten Dateien gefunden.",
        "overwrite":       "Vorhandene Ausgabedateien überschreiben?",
        "yes":             "Ja",
        "no":              "Nein",
        "back":            "Zurück",
        "strl_warn":       "strL-Spalten erkannt — werden für alte Stata-Formate auf str2045 gekürzt",
        "label_warn":      "Variablenlabels für Zielformat gekürzt",
        "summary":         "Konvertierungsübersicht",
        "converted":       "Konvertiert",
        "skipped":         "Übersprungen",
        "failed":          "Fehlgeschlagen",
        "press_enter":     "Enter drücken um fortzufahren...",
        "select_files":    "Dateipfade eingeben (kommagetrennt oder Glob, z.B. *.dta)",
        "incl_subdir":     "Unterordner einschließen?",
        "filter_ext":      "Nach Erweiterung filtern (leer lassen für alle unterstützten)",
        "settings_title":  "Einstellungen",
        "cur_lang":        "Aktuelle Sprache",
        "change_lang":     "Sprache ändern",
        "default_out_fmt": "Standard-Ausgabeformat",
        "default_stata_v": "Standard-Stata-Version",
        "version_table":   "Stata-Versionsreferenz",
        "file_num":        "Datei",
        "of":              "von",
        "browse_hint":     "Tipp: Dateien/Ordner können per Drag & Drop ins Terminal gezogen werden",
    },
    "fr": {
        "welcome":         "Bienvenue dans StataBridge — Convertisseur universel de données Stata",
        "select_lang":     "Choisir la langue:",
        "main_menu":       "Menu principal",
        "opt_single":      "Convertir un seul fichier",
        "opt_multiple":    "Convertir plusieurs fichiers (sélectionner des fichiers)",
        "opt_batch":       "Conversion par lot du dossier entier",
        "opt_settings":    "Paramètres",
        "opt_quit":        "Quitter",
        "choose":          "Votre choix",
        "input_file":      "Chemin du fichier d'entrée",
        "input_dir":       "Chemin du dossier d'entrée",
        "output_dir":      "Chemin du dossier de sortie (vide = même que l'entrée)",
        "output_format":   "Format de sortie",
        "stata_version":   "Version Stata cible",
        "converting":      "Conversion en cours",
        "done":            "Terminé!",
        "error":           "Erreur",
        "success":         "Succès",
        "files_found":     "fichiers trouvés",
        "no_files":        "Aucun fichier pris en charge trouvé.",
        "overwrite":       "Écraser les fichiers de sortie existants?",
        "yes":             "Oui",
        "no":              "Non",
        "back":            "Retour",
        "strl_warn":       "Colonnes strL détectées — seront tronquées en str2045 pour l'ancien format Stata",
        "label_warn":      "Étiquettes de variables tronquées pour le format cible",
        "summary":         "Résumé de la conversion",
        "converted":       "Convertis",
        "skipped":         "Ignorés",
        "failed":          "Échoués",
        "press_enter":     "Appuyez sur Entrée pour continuer...",
        "select_files":    "Entrez les chemins (séparés par virgule ou glob, ex. *.dta)",
        "incl_subdir":     "Inclure les sous-dossiers?",
        "filter_ext":      "Filtrer par extension (vide pour tous les formats supportés)",
        "settings_title":  "Paramètres",
        "cur_lang":        "Langue actuelle",
        "change_lang":     "Changer la langue",
        "default_out_fmt": "Format de sortie par défaut",
        "default_stata_v": "Version Stata par défaut",
        "version_table":   "Référence des versions Stata",
        "file_num":        "Fichier",
        "of":              "de",
        "browse_hint":     "Astuce: vous pouvez glisser-déposer des fichiers/dossiers dans le terminal",
    },
    "es": {
        "welcome":         "Bienvenido a StataBridge — Convertidor universal de datos Stata",
        "select_lang":     "Seleccionar idioma:",
        "main_menu":       "Menú principal",
        "opt_single":      "Convertir un solo archivo",
        "opt_multiple":    "Convertir múltiples archivos (seleccionar archivos)",
        "opt_batch":       "Conversión por lotes de toda la carpeta",
        "opt_settings":    "Configuración",
        "opt_quit":        "Salir",
        "choose":          "Su elección",
        "input_file":      "Ruta del archivo de entrada",
        "input_dir":       "Ruta de la carpeta de entrada",
        "output_dir":      "Ruta de la carpeta de salida (vacío = igual que entrada)",
        "output_format":   "Formato de salida",
        "stata_version":   "Versión de Stata objetivo",
        "converting":      "Convirtiendo",
        "done":            "¡Listo!",
        "error":           "Error",
        "success":         "Éxito",
        "files_found":     "archivos encontrados",
        "no_files":        "No se encontraron archivos compatibles.",
        "overwrite":       "¿Sobrescribir archivos de salida existentes?",
        "yes":             "Sí",
        "no":              "No",
        "back":            "Volver",
        "strl_warn":       "Columnas strL detectadas — se truncarán a str2045 para el formato Stata antiguo",
        "label_warn":      "Etiquetas de variables truncadas para el formato destino",
        "summary":         "Resumen de conversión",
        "converted":       "Convertidos",
        "skipped":         "Omitidos",
        "failed":          "Fallidos",
        "press_enter":     "Presione Enter para continuar...",
        "select_files":    "Ingrese rutas de archivos (separadas por coma o glob, ej. *.dta)",
        "incl_subdir":     "¿Incluir subdirectorios?",
        "filter_ext":      "Filtrar por extensión (vacío para todos los soportados)",
        "settings_title":  "Configuración",
        "cur_lang":        "Idioma actual",
        "change_lang":     "Cambiar idioma",
        "default_out_fmt": "Formato de salida predeterminado",
        "default_stata_v": "Versión de Stata predeterminada",
        "version_table":   "Referencia de versiones de Stata",
        "file_num":        "Archivo",
        "of":              "de",
        "browse_hint":     "Consejo: puede arrastrar y soltar archivos/carpetas en el terminal",
    },
}
# Fill in missing languages by falling back to English
for _lang_code in ["it", "pt", "ja", "zh"]:
    T[_lang_code] = T["en"].copy()

T["it"]["welcome"]     = "Benvenuto in StataBridge — Convertitore universale di dati Stata"
T["pt"]["welcome"]     = "Bem-vindo ao StataBridge — Conversor universal de dados Stata"
T["ja"]["welcome"]     = "StataBridgeへようこそ — 汎用Stataデータコンバータ"
T["zh"]["welcome"]     = "欢迎使用 StataBridge — 通用 Stata 数据转换器"

# ─────────────────────────────────────────────
#  STATA VERSION MAP
#  (format_number, pandas_version_param, description)
# ─────────────────────────────────────────────

# STATA_VERSIONS: (display_format_num, pandas_write_version, description)
# pandas to_stata only accepts version= 114, 117, 118, 119.
# We use 114 for all pre-13 targets, 117 for Stata 13, 118 for 14-15, 119 for 16+.
# Format 115 (Stata 13 native) and 108/113 (Stata 7/8) cannot be written by pandas;
# we map them to the closest compatible writable format with a note.
STATA_VERSIONS = {
    "Stata 7  (format 108 → writes 114)": (108, 114,
        "Stata 7/8 — pandas writes format 114; readable by Stata 7+"),
    "Stata 8  (format 113 → writes 114)": (113, 114,
        "Stata 8/9 — pandas writes format 114; readable by Stata 7+"),
    "Stata 9  (format 114)":  (114, 114, "Stata 9/10/11/12 — Latin-1, str≤244"),
    "Stata 10 (format 114)":  (114, 114, "Stata 10 — Latin-1, str≤244"),
    "Stata 11 (format 114)":  (114, 114, "Stata 11 — Latin-1, str≤244"),
    "Stata 12 (format 114)":  (114, 114, "Stata 12 — Latin-1, str≤244"),
    "Stata 13 (format 117)":  (117, 117, "Stata 13 — strL support, Latin-1"),
    "Stata 14 (format 118)":  (118, 118, "Stata 14/15 — Unicode (UTF-8), strL"),
    "Stata 15 (format 118)":  (118, 118, "Stata 15 — Unicode (UTF-8), strL"),
    "Stata 16 (format 119)":  (119, 119, "Stata 16 — Unicode, >32,767 vars"),
    "Stata 17 (format 119)":  (119, 119, "Stata 17 — Unicode, >32,767 vars"),
    "Stata 18 (format 119)":  (119, 119, "Stata 18 — Unicode, >32,767 vars"),
    "Stata 19 (format 119)":  (119, 119, "Stata 19 — Unicode, >32,767 vars (pandas max)"),
}

# Max string length per pandas write version (for strL / old-format fallback)
STATA_MAX_STR = {
    114: 244,   # Stata 7-12
    117: 2045,  # Stata 13
    118: 2045,  # Stata 14-15
    119: 2045,  # Stata 16-19
}

SUPPORTED_INPUT_EXTS = {".dta", ".csv", ".xlsx", ".xls", ".dbf", ".rdata", ".rda", ".rds"}
SUPPORTED_OUTPUT_FMTS = ["dta (Stata)", "csv", "xlsx", "dbf"]

# ─────────────────────────────────────────────
#  ANSI COLORS
# ─────────────────────────────────────────────
class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"
    BG_BLUE = "\033[44m"
    BG_DARK = "\033[40m"

def supports_color():
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

def colored(text, *codes):
    if not supports_color():
        return text
    return "".join(codes) + str(text) + C.RESET

# ─────────────────────────────────────────────
#  UI HELPERS
# ─────────────────────────────────────────────
def clear():
    os.system("cls" if os.name == "nt" else "clear")

def banner(lang_code="en"):
    txt = T[lang_code]["welcome"]
    width = max(len(txt) + 4, 60)
    bar = "═" * width
    print(colored(f"\n╔{bar}╗", C.CYAN, C.BOLD))
    print(colored(f"║  {txt.center(width - 2)}  ║", C.CYAN, C.BOLD))
    print(colored(f"╚{bar}╝\n", C.CYAN, C.BOLD))

def section(title):
    width = 50
    print(colored(f"\n{'─'*width}", C.BLUE))
    print(colored(f"  {title}", C.BOLD, C.WHITE))
    print(colored(f"{'─'*width}", C.BLUE))

def ok(msg):
    print(colored(f"  ✓ {msg}", C.GREEN))

def warn(msg):
    print(colored(f"  ⚠ {msg}", C.YELLOW))

def err(msg):
    print(colored(f"  ✗ {msg}", C.RED))

def info(msg):
    print(colored(f"  → {msg}", C.CYAN))

def ask(prompt, default=""):
    hint = f" [{default}]" if default else ""
    try:
        val = input(colored(f"  {prompt}{hint}: ", C.BOLD, C.WHITE)).strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return default
    return val if val else default

def ask_yn(prompt, lang_code="en"):
    yes = T[lang_code]["yes"][0].lower()
    no  = T[lang_code]["no"][0].lower()
    while True:
        val = ask(f"{prompt} ({T[lang_code]['yes']}/{T[lang_code]['no']})").lower()
        if val in (yes, T[lang_code]["yes"].lower()):
            return True
        if val in (no, T[lang_code]["no"].lower(), ""):
            return False

def menu(title, options, lang_code="en", show_back=True, show_quit=True):
    """Display numbered menu, return chosen index (0-based) or -1 for back, -2 for quit."""
    extended = list(options)
    if show_back:
        extended.append(f"← {T[lang_code]['back']}")
    if show_quit:
        extended.append(f"✕  {T[lang_code]['opt_quit']}")
    section(title)
    for i, opt in enumerate(extended, 1):
        num = colored(f"  [{i}]", C.YELLOW, C.BOLD)
        print(f"{num} {opt}")
    print()
    while True:
        raw = ask(T[lang_code]["choose"])
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(options):
                return idx
            if show_back and idx == len(options):
                return -1
            if show_quit and idx == len(extended) - 1:
                return -2
        print(colored(f"  Please enter 1–{len(extended)}", C.RED))

def press_enter(lang_code="en"):
    try:
        input(colored(f"\n  {T[lang_code]['press_enter']}", C.DIM))
    except (EOFError, KeyboardInterrupt):
        pass

def progress_bar(current, total, width=30):
    pct = current / total if total else 1
    filled = int(pct * width)
    bar = "█" * filled + "░" * (width - filled)
    return colored(f"[{bar}] {current}/{total}", C.CYAN)

# ─────────────────────────────────────────────
#  FILE DISCOVERY
# ─────────────────────────────────────────────
def expand_paths(raw_input: str) -> list:
    """Expand comma-separated paths, globs, directories."""
    parts = [p.strip() for p in raw_input.split(",") if p.strip()]
    found = []
    for part in parts:
        # Remove surrounding quotes that terminals might add on drag-drop
        part = part.strip("'\"")
        if "*" in part or "?" in part:
            found.extend(glob.glob(part, recursive=True))
        elif os.path.isfile(part):
            found.append(part)
        elif os.path.isdir(part):
            # treat as dir, let caller handle
            found.append(part)
        else:
            warn(f"Not found: {part}")
    return found

def collect_files_in_dir(directory: str, recursive: bool = False,
                          ext_filter: set = None) -> list:
    exts = ext_filter or SUPPORTED_INPUT_EXTS
    found = []
    if recursive:
        for root, _, files in os.walk(directory):
            for f in files:
                if Path(f).suffix.lower() in exts:
                    found.append(os.path.join(root, f))
    else:
        for f in os.listdir(directory):
            fp = os.path.join(directory, f)
            if os.path.isfile(fp) and Path(f).suffix.lower() in exts:
                found.append(fp)
    return sorted(found)

# ─────────────────────────────────────────────
#  CORE CONVERSION ENGINE
# ─────────────────────────────────────────────
def _sanitize_column_names(df):
    """Make column names Stata-safe (32 chars, alphanumeric+_, starts with letter)."""
    import re
    new_cols = {}
    seen = set()
    for col in df.columns:
        s = str(col)
        s = re.sub(r"[^a-zA-Z0-9_]", "_", s)
        if s and s[0].isdigit():
            s = "v_" + s
        if not s:
            s = "var"
        s = s[:32]
        base = s
        n = 1
        while s in seen:
            suffix = f"_{n}"
            s = base[:32 - len(suffix)] + suffix
            n += 1
        seen.add(s)
        new_cols[col] = s
    df = df.rename(columns=new_cols)
    return df

def _handle_unicode_for_old_stata(df, pandas_ver: int, warnings: list):
    """Old Stata formats (<=115) use Latin-1 encoding; replace non-Latin-1 chars."""
    if pandas_ver >= 118:
        return df
    import pandas as pd
    for col in df.select_dtypes(include="object").columns:
        def _safe_latin1(v):
            if not isinstance(v, str):
                return v
            return v.encode("latin-1", errors="replace").decode("latin-1")
        n_bad = df[col].dropna().apply(
            lambda v: any(ord(c) > 255 for c in str(v)) if isinstance(v, str) else False
        ).sum()
        if n_bad > 0:
            df[col] = df[col].apply(_safe_latin1)
            warnings.append(
                f"Column '{col}': {n_bad} values had non-Latin-1 chars (replaced with '?') "
                f"— old Stata write version {pandas_ver} does not support Unicode"
            )
    return df

def _handle_strl_fallback(df, target_format: int, warnings: list):
    """Convert object columns that may contain strL (long strings) to truncated str."""
    max_str = STATA_MAX_STR.get(target_format, 2045)
    for col in df.select_dtypes(include="object").columns:
        # Check if any value exceeds max_str
        max_len = df[col].dropna().astype(str).str.len().max() if len(df) > 0 else 0
        if max_len > max_str:
            df[col] = df[col].astype(str).str[:max_str]
            warnings.append(f"Column '{col}': truncated to {max_str} chars")
    return df

def _truncate_labels(labels: dict, max_len: int) -> dict:
    return {k: str(v)[:max_len] for k, v in labels.items()}


def _read_dbf_fallback(path: str):
    """Pure-Python dBASE III/IV reader — no external library needed."""
    import struct, datetime as _dt
    import pandas as pd

    with open(path, 'rb') as f:
        raw = f.read()

    # Header
    n_records  = struct.unpack_from('<I', raw, 4)[0]
    header_len = struct.unpack_from('<H', raw, 8)[0]
    record_len = struct.unpack_from('<H', raw, 10)[0]

    # Field descriptors (32 bytes each, starting at offset 32, end at 0x0D)
    fields = []
    offset = 32
    while raw[offset] != 0x0D and offset < header_len:
        name  = raw[offset:offset+11].rstrip(b'').decode('latin-1', errors='replace').strip().strip(chr(0))
        ftype = chr(raw[offset+11])
        flen  = raw[offset+16]
        fdec  = raw[offset+17]
        fields.append((name, ftype, flen, fdec))
        offset += 32

    # Records
    rows = []
    rec_start = header_len
    for i in range(n_records):
        rec_offset = rec_start + i * record_len
        if rec_offset + record_len > len(raw):
            break
        deletion_flag = raw[rec_offset]
        if deletion_flag == 0x2A:   # deleted record
            continue
        col_offset = rec_offset + 1
        row = {}
        for name, ftype, flen, fdec in fields:
            raw_val = raw[col_offset:col_offset + flen]
            col_offset += flen
            text = raw_val.decode('latin-1', errors='replace').strip()
            if ftype == 'C':
                row[name] = text
            elif ftype == 'N' or ftype == 'F':
                try:
                    row[name] = float(text) if '.' in text else int(text)
                except ValueError:
                    row[name] = None
            elif ftype == 'D':
                try:
                    row[name] = _dt.date(int(text[:4]), int(text[4:6]), int(text[6:8]))
                except Exception:
                    row[name] = None
            elif ftype == 'L':
                row[name] = True if text.upper() in ('T','Y') else (False if text.upper() in ('F','N') else None)
            else:
                row[name] = text
        rows.append(row)

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[f[0] for f in fields])

def _read_file(path: str) -> tuple:
    """Returns (DataFrame, variable_labels, value_labels, metadata_dict)."""
    import pandas as pd
    ext = Path(path).suffix.lower()
    meta = {}
    var_labels = {}
    val_labels = {}

    if ext == ".dta":
        reader = pd.io.stata.StataReader(path)
        df = reader.read()
        try:
            var_labels = reader.variable_labels()
        except Exception:
            pass
        try:
            val_labels = reader.value_labels()
        except Exception:
            pass
        try:
            meta["data_label"] = reader.data_label
        except Exception:
            pass
        try:
            reader.close()
        except Exception:
            pass

    elif ext == ".csv":
        df = pd.read_csv(path, low_memory=False)

    elif ext in (".xlsx", ".xls"):
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        try:
            df = pd.read_excel(path, engine=engine)
        except Exception:
            df = pd.read_excel(path)

    elif ext == ".dbf":
        try:
            import dbfread
            table = dbfread.DBF(path, load=True)
            df = pd.DataFrame(iter(table))
        except ImportError:
            # Pure-Python fallback for dBASE III/IV DBF files
            df = _read_dbf_fallback(path)

    elif ext in (".rdata", ".rda", ".rds"):
        try:
            import rpy2.robjects as ro
            from rpy2.robjects import pandas2ri
            pandas2ri.activate()
            if ext == ".rds":
                r_obj = ro.r["readRDS"](path)
            else:
                ro.r["load"](path)
                # Get the first data.frame in workspace
                varnames = list(ro.r["ls"]())
                r_obj = ro.r[varnames[0]]
            df = pandas2ri.rpy2py(r_obj)
        except ImportError:
            raise RuntimeError(
                "rpy2 not installed (or R not available). "
                "Install with: pip install rpy2  (requires R)"
            )
    else:
        raise ValueError(f"Unsupported input format: {ext}")

    return df, var_labels, val_labels, meta

def _write_file(df, output_path: str, fmt: str, stata_version_key: str,
                var_labels: dict, val_labels: dict, meta: dict,
                warnings: list):
    import pandas as pd
    ext = Path(output_path).suffix.lower()
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    if fmt == "dta (Stata)":
        fmt_num, pandas_ver, _ = STATA_VERSIONS[stata_version_key]
        df = _sanitize_column_names(df)
        df = _handle_strl_fallback(df, pandas_ver, warnings)
        df = _handle_unicode_for_old_stata(df, pandas_ver, warnings)

        # Truncate labels for old formats
        label_max = 80 if fmt_num <= 115 else 320
        vl = _truncate_labels(var_labels, label_max) if var_labels else None

        # Align variable_labels keys to renamed columns
        write_kwargs = dict(
            path=output_path,
            version=pandas_ver,
            write_index=False,
            time_stamp=datetime.datetime.now(),
        )
        if vl:
            # Filter to only keys that exist in df
            vl = {k: v for k, v in vl.items() if k in df.columns}
            if vl:
                write_kwargs["variable_labels"] = vl
        if meta.get("data_label"):
            dl = str(meta["data_label"])[:80]
            write_kwargs["data_label"] = dl
        if val_labels and pandas_ver >= 117:
            write_kwargs["value_labels"] = val_labels

        df.to_stata(**write_kwargs)

    elif fmt == "csv":
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

    elif fmt == "xlsx":
        df.to_excel(output_path, index=False, engine="openpyxl")

    elif fmt == "dbf":
        try:
            _write_dbf(df, output_path)
        except Exception as e:
            raise RuntimeError(f"DBF write failed: {e}")

def _write_dbf(df, path: str):
    """Write DataFrame to DBF using struct — no external library needed."""
    import struct, itertools
    # DBF only supports limited types; coerce everything
    import pandas as pd
    rows = []
    fields = []
    for col in df.columns:
        cname = str(col)[:10].upper().encode("ascii", errors="replace")
        series = df[col]
        if pd.api.types.is_integer_dtype(series):
            fields.append((cname, b"N", 11, 0))
            rows.append(series.fillna(0).astype(int).astype(str).str.rjust(11).str[:11])
        elif pd.api.types.is_float_dtype(series):
            fields.append((cname, b"N", 20, 6))
            rows.append(series.fillna(0).apply(lambda x: f"{x:20.6f}"[:20]))
        elif pd.api.types.is_datetime64_any_dtype(series):
            fields.append((cname, b"D", 8, 0))
            rows.append(series.dt.strftime("%Y%m%d").fillna("        ").str[:8])
        else:
            max_len = min(int(series.astype(str).str.len().max() or 1), 254)
            fields.append((cname, b"C", max_len, 0))
            rows.append(series.fillna("").astype(str).str[:max_len].str.ljust(max_len))

    header_size = 32 + 32 * len(fields) + 1
    record_size = sum(f[2] for f in fields) + 1
    n_records = len(df)
    now = datetime.date.today()

    with open(path, "wb") as f:
        # Header
        f.write(struct.pack("B", 3))  # version
        f.write(struct.pack("3B", now.year - 1900, now.month, now.day))
        f.write(struct.pack("<I", n_records))
        f.write(struct.pack("<H", header_size))
        f.write(struct.pack("<H", record_size))
        f.write(b"\x00" * 20)  # reserved
        # Field descriptors
        for cname, ftype, flen, fdec in fields:
            fname_padded = cname.ljust(11, b"\x00")[:11]
            f.write(fname_padded)
            f.write(ftype)
            f.write(b"\x00" * 4)  # reserved
            f.write(struct.pack("B", flen))
            f.write(struct.pack("B", fdec))
            f.write(b"\x00" * 14)
        f.write(b"\r")  # header terminator
        # Records
        col_data = [r.tolist() for r in rows]
        for i in range(n_records):
            f.write(b" ")  # deletion flag
            for j, (_, ftype, flen, _) in enumerate(fields):
                val = str(col_data[j][i])
                if ftype == b"N":
                    f.write(val.encode("ascii", errors="replace").rjust(flen)[:flen])
                else:
                    f.write(val.encode("latin-1", errors="replace").ljust(flen)[:flen])
        f.write(b"\x1a")  # EOF

def convert_file(input_path: str, output_path: str, fmt: str,
                 stata_version_key: str, overwrite: bool = True) -> dict:
    """Main conversion function. Returns result dict."""
    result = {"input": input_path, "output": output_path,
              "status": "ok", "warnings": [], "error": None}
    try:
        if not overwrite and os.path.exists(output_path):
            result["status"] = "skipped"
            return result

        df, var_labels, val_labels, meta = _read_file(input_path)
        _write_file(df, output_path, fmt, stata_version_key,
                    var_labels, val_labels, meta, result["warnings"])
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    return result

# ─────────────────────────────────────────────
#  OUTPUT PATH BUILDER
# ─────────────────────────────────────────────
def build_output_path(input_path: str, output_dir: Optional[str],
                      fmt: str, base_input_dir: Optional[str] = None) -> str:
    ext_map = {
        "dta (Stata)": ".dta",
        "csv":         ".csv",
        "xlsx":        ".xlsx",
        "dbf":         ".dbf",
    }
    new_ext = ext_map.get(fmt, ".dta")
    stem = Path(input_path).stem
    if output_dir:
        if base_input_dir:
            # Preserve sub-directory structure relative to base
            try:
                rel = Path(input_path).parent.relative_to(base_input_dir)
                out_folder = Path(output_dir) / rel
            except ValueError:
                out_folder = Path(output_dir)
        else:
            out_folder = Path(output_dir)
        return str(out_folder / (stem + new_ext))
    else:
        return str(Path(input_path).parent / (stem + new_ext))

# ─────────────────────────────────────────────
#  SHARED CONVERSION FLOW
# ─────────────────────────────────────────────
def run_conversions(files: list, output_dir: Optional[str], fmt: str,
                    stata_version_key: str, overwrite: bool,
                    lang_code: str, base_input_dir: Optional[str] = None):
    results = []
    total = len(files)
    for i, fp in enumerate(files, 1):
        out = build_output_path(fp, output_dir, fmt, base_input_dir)
        fname = os.path.basename(fp)
        print(f"\n  {progress_bar(i, total)}  {colored(fname, C.BOLD)}")
        info(f"{T[lang_code]['converting']}: {fp}")
        info(f"→ {out}")
        r = convert_file(fp, out, fmt, stata_version_key, overwrite)
        for w in r["warnings"]:
            warn(w)
        if r["status"] == "ok":
            ok(T[lang_code]["success"])
        elif r["status"] == "skipped":
            warn(T[lang_code]["skipped"])
        else:
            err(f"{T[lang_code]['error']}: {r['error']}")
        results.append(r)

    # Summary
    n_ok  = sum(1 for r in results if r["status"] == "ok")
    n_sk  = sum(1 for r in results if r["status"] == "skipped")
    n_err = sum(1 for r in results if r["status"] == "error")
    section(T[lang_code]["summary"])
    ok(f"{T[lang_code]['converted']}: {n_ok}")
    if n_sk:
        warn(f"{T[lang_code]['skipped']}: {n_sk}")
    if n_err:
        err(f"{T[lang_code]['failed']}: {n_err}")
        for r in results:
            if r["status"] == "error":
                err(f"  {os.path.basename(r['input'])}: {r['error']}")
    return results

# ─────────────────────────────────────────────
#  OUTPUT FORMAT + VERSION SELECTION
# ─────────────────────────────────────────────
def ask_output_format(lang_code, settings):
    fmts = SUPPORTED_OUTPUT_FMTS
    default_idx = fmts.index(settings["default_output_format"])
    section(T[lang_code]["output_format"])
    for i, f in enumerate(fmts, 1):
        marker = colored(" ◀ default", C.DIM) if i - 1 == default_idx else ""
        print(f"  [{colored(i, C.YELLOW, C.BOLD)}] {f}{marker}")
    raw = ask(T[lang_code]["choose"], str(default_idx + 1))
    idx = int(raw) - 1 if raw.isdigit() and 0 <= int(raw) - 1 < len(fmts) else default_idx
    return fmts[idx]

def ask_stata_version(lang_code, settings):
    keys = list(STATA_VERSIONS.keys())
    default = settings["default_stata_version"]
    default_idx = keys.index(default) if default in keys else 13  # Stata 18
    section(T[lang_code]["stata_version"])
    for i, k in enumerate(keys, 1):
        marker = colored(" ◀ default", C.DIM) if i - 1 == default_idx else ""
        desc = STATA_VERSIONS[k][2]
        print(f"  [{colored(str(i).rjust(2), C.YELLOW, C.BOLD)}] {k:<32} {colored(desc, C.DIM)}{marker}")
    raw = ask(T[lang_code]["choose"], str(default_idx + 1))
    idx = int(raw) - 1 if raw.isdigit() and 0 <= int(raw) - 1 < len(keys) else default_idx
    return keys[idx]

# ─────────────────────────────────────────────
#  SCREENS
# ─────────────────────────────────────────────
def screen_single(lang_code, settings):
    section(T[lang_code]["opt_single"])
    info(T[lang_code]["browse_hint"])
    raw = ask(T[lang_code]["input_file"])
    raw = raw.strip("'\"")
    if not os.path.isfile(raw):
        err(f"{T[lang_code]['error']}: file not found: {raw}")
        press_enter(lang_code)
        return

    fmt = ask_output_format(lang_code, settings)
    stata_ver = None
    if fmt == "dta (Stata)":
        stata_ver = ask_stata_version(lang_code, settings)
    else:
        stata_ver = settings["default_stata_version"]

    out_dir_raw = ask(T[lang_code]["output_dir"]).strip("'\"")
    out_dir = out_dir_raw if out_dir_raw else None
    overwrite = ask_yn(T[lang_code]["overwrite"], lang_code)

    run_conversions([raw], out_dir, fmt, stata_ver, overwrite, lang_code)
    press_enter(lang_code)

def screen_multiple(lang_code, settings):
    section(T[lang_code]["opt_multiple"])
    info(T[lang_code]["browse_hint"])
    raw = ask(T[lang_code]["select_files"])
    paths = expand_paths(raw)
    files = [p for p in paths if os.path.isfile(p) and
             Path(p).suffix.lower() in SUPPORTED_INPUT_EXTS]
    if not files:
        warn(T[lang_code]["no_files"])
        press_enter(lang_code)
        return
    print()
    ok(f"{len(files)} {T[lang_code]['files_found']}")
    for f in files:
        print(colored(f"    {f}", C.DIM))

    fmt = ask_output_format(lang_code, settings)
    stata_ver = ask_stata_version(lang_code, settings) if fmt == "dta (Stata)" else settings["default_stata_version"]
    out_dir_raw = ask(T[lang_code]["output_dir"]).strip("'\"")
    out_dir = out_dir_raw if out_dir_raw else None
    overwrite = ask_yn(T[lang_code]["overwrite"], lang_code)

    run_conversions(files, out_dir, fmt, stata_ver, overwrite, lang_code)
    press_enter(lang_code)

def screen_batch(lang_code, settings, recursive=False):
    section(T[lang_code]["opt_batch"])
    info(T[lang_code]["browse_hint"])
    raw = ask(T[lang_code]["input_dir"]).strip("'\"")
    if not os.path.isdir(raw):
        err(f"{T[lang_code]['error']}: folder not found: {raw}")
        press_enter(lang_code)
        return

    if not recursive:
        recursive = ask_yn(T[lang_code]["incl_subdir"], lang_code)

    ext_raw = ask(T[lang_code]["filter_ext"]).strip().lower()
    if ext_raw:
        ext_filter = {("." + e.lstrip(".")) for e in ext_raw.replace(",", " ").split()}
    else:
        ext_filter = SUPPORTED_INPUT_EXTS

    files = collect_files_in_dir(raw, recursive, ext_filter)
    if not files:
        warn(T[lang_code]["no_files"])
        press_enter(lang_code)
        return
    ok(f"{len(files)} {T[lang_code]['files_found']}")
    for f in files[:10]:
        print(colored(f"    {f}", C.DIM))
    if len(files) > 10:
        print(colored(f"    ... and {len(files)-10} more", C.DIM))

    fmt = ask_output_format(lang_code, settings)
    stata_ver = ask_stata_version(lang_code, settings) if fmt == "dta (Stata)" else settings["default_stata_version"]
    out_dir_raw = ask(T[lang_code]["output_dir"]).strip("'\"")
    out_dir = out_dir_raw if out_dir_raw else None
    overwrite = ask_yn(T[lang_code]["overwrite"], lang_code)

    run_conversions(files, out_dir, fmt, stata_ver, overwrite, lang_code,
                    base_input_dir=raw if recursive else None)
    press_enter(lang_code)

def screen_settings(lang_code, settings):
    while True:
        section(T[lang_code]["settings_title"])
        fmts = SUPPORTED_OUTPUT_FMTS
        stata_keys = list(STATA_VERSIONS.keys())
        opts = [
            f"{T[lang_code]['cur_lang']}: {colored(lang_code.upper(), C.CYAN)}",
            f"{T[lang_code]['default_out_fmt']}: {colored(settings['default_output_format'], C.CYAN)}",
            f"{T[lang_code]['default_stata_v']}: {colored(settings['default_stata_version'], C.CYAN)}",
            T[lang_code]["version_table"],
        ]
        choice = menu(T[lang_code]["settings_title"], opts, lang_code,
                      show_back=True, show_quit=False)
        if choice == -1:
            break
        elif choice == 0:
            # Change language
            lang_names = list(LANGUAGES.keys())
            for i, ln in enumerate(lang_names, 1):
                print(f"  [{colored(i, C.YELLOW, C.BOLD)}] {ln} ({LANGUAGES[ln]})")
            raw = ask(T[lang_code]["choose"])
            if raw.isdigit() and 1 <= int(raw) <= len(lang_names):
                new_lc = LANGUAGES[lang_names[int(raw) - 1]]
                settings["lang_code"] = new_lc
                lang_code = new_lc
        elif choice == 1:
            for i, f in enumerate(fmts, 1):
                print(f"  [{colored(i, C.YELLOW, C.BOLD)}] {f}")
            raw = ask(T[lang_code]["choose"])
            if raw.isdigit() and 1 <= int(raw) <= len(fmts):
                settings["default_output_format"] = fmts[int(raw) - 1]
        elif choice == 2:
            for i, k in enumerate(stata_keys, 1):
                print(f"  [{colored(str(i).rjust(2), C.YELLOW, C.BOLD)}] {k}")
            raw = ask(T[lang_code]["choose"])
            if raw.isdigit() and 1 <= int(raw) <= len(stata_keys):
                settings["default_stata_version"] = stata_keys[int(raw) - 1]
        elif choice == 3:
            section(T[lang_code]["version_table"])
            print(colored(f"  {'Stata Version':<25} {'Format':<8} {'Notes'}", C.BOLD))
            print(colored(f"  {'─'*60}", C.DIM))
            for k, (fmt_num, pandas_ver, notes) in STATA_VERSIONS.items():
                stata_label = k.split("(")[0].strip()
                print(f"  {stata_label:<25} {fmt_num:<8} {colored(notes, C.DIM)}")
            print()
            print(colored("  strL (long strings) support: Stata 13+ (format 117+)", C.YELLOW))
            print(colored("  Unicode support: Stata 14+ (format 118+)", C.YELLOW))
            press_enter(lang_code)
    return lang_code

# ─────────────────────────────────────────────
#  SELECT LANGUAGE SCREEN
# ─────────────────────────────────────────────
def select_language():
    lang_names = list(LANGUAGES.keys())
    print(colored(f"\n  {T['en']['select_lang']}", C.BOLD, C.WHITE))
    for i, ln in enumerate(lang_names, 1):
        print(f"  [{colored(i, C.YELLOW, C.BOLD)}] {ln} ({LANGUAGES[ln]})")
    print(f"  [↵] English (default)")
    raw = input(colored(f"\n  {T['en']['choose']}: ", C.BOLD, C.WHITE)).strip()
    if raw.isdigit() and 1 <= int(raw) <= len(lang_names):
        return LANGUAGES[lang_names[int(raw) - 1]]
    return "en"

# ─────────────────────────────────────────────
#  MAIN LOOP
# ─────────────────────────────────────────────
def main():
    clear()
    banner("en")
    lang_code = select_language()
    clear()
    banner(lang_code)

    settings = {
        "lang_code":             lang_code,
        "default_output_format": "dta (Stata)",
        "default_stata_version": "Stata 18 (format 119)",
    }

    while True:
        clear()
        banner(lang_code)
        opts = [
            T[lang_code]["opt_single"],
            T[lang_code]["opt_multiple"],
            T[lang_code]["opt_batch"],
            T[lang_code]["opt_settings"],
        ]
        choice = menu(T[lang_code]["main_menu"], opts, lang_code,
                      show_back=False, show_quit=True)
        if choice == -2:
            print(colored("\n  Goodbye / Sampai jumpa / Auf Wiedersehen / Au revoir!\n", C.CYAN))
            break
        elif choice == 0:
            screen_single(lang_code, settings)
        elif choice == 1:
            screen_multiple(lang_code, settings)
        elif choice == 2:
            screen_batch(lang_code, settings)
        elif choice == 3:
            lang_code = screen_settings(lang_code, settings)
            settings["lang_code"] = lang_code

if __name__ == "__main__":
    main()

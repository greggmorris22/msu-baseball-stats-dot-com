"""
convert_heic.py
Converts HEIC/HEIF image files to JPG for web browser compatibility.
Chrome on Windows does not support HEIC natively.

Requires: pip install pillow pillow-heif
"""

import sys
from pathlib import Path

try:
    import pillow_heif
    from PIL import Image
except ImportError:
    print("Missing dependencies. Run:")
    print("  pip install pillow pillow-heif")
    sys.exit(1)

# Register HEIC/HEIF support with Pillow
pillow_heif.register_heif_opener()

PHOTOS_DIR = Path(__file__).parent.parent / "public" / "photos"

# Files to convert: (source filename, output filename)
TO_CONVERT = [
    ("Baby Name - Aviaiton.HEIC",    "Baby Name - Aviation.jpg"),
    ("Retina Scan (fundus).heic",    "Retina Scan (fundus).jpg"),
    ("Astros Retro Logo.HEIC",       "Astros Retro Logo.jpg"),
]

for src_name, dst_name in TO_CONVERT:
    src = PHOTOS_DIR / src_name
    dst = PHOTOS_DIR / dst_name

    if not src.exists():
        print(f"  SKIP (not found): {src_name}")
        continue

    print(f"  Converting: {src_name} -> {dst_name}")
    img = Image.open(src)
    img = img.convert("RGB")          # HEIC can be RGBA; JPG doesn't support alpha
    img.save(dst, "JPEG", quality=92)
    print(f"  Saved: {dst}")

print("\nDone.")

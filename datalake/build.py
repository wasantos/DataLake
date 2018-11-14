#!/usr/bin/env python3

import shutil
import subprocess
import sys
import os
import zipfile

from pathlib import Path

# Set filename
filename = 'datalake.zip'
args = sys.argv[1:]
if len(args) > 0:
    filename = args[0]

# Set directories
current_dir: Path = Path(__file__).resolve().parent
build_dir: Path = current_dir / '_build'

# Ensure clean folder exists
shutil.rmtree(str(build_dir), ignore_errors=True)
build_dir.mkdir()

# Install requirements
subprocess.check_call([sys.executable, '-m', 'pip', 'install',
    '-r', str(current_dir / 'requirements.txt'),
    '-t', str(build_dir)
])

# Copy source files
for pyfile in current_dir.glob("*.py"):
    src = str(pyfile)
    dst = str(build_dir)
    shutil.copy(src, dst)

# Generate zip file
zipped = current_dir / filename
if zipped.exists():
    os.remove(zipped)

with zipfile.ZipFile(str(zipped), 'w') as zf:
    for path in build_dir.glob('**/*'):
        if path.is_file():
            zipname = path.relative_to(build_dir)
            zf.write(str(path), str(path.relative_to(build_dir)))

print(f"Done, check {zipped}")

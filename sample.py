# generate_sample_2mb.py
import os

size = 2 * 1024 * 1024   # 2 MB
content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n"

with open("sample_2mb.txt", "w") as f:
    written = 0
    while written < size:
        f.write(content)
        written += len(content)

print("Generated sample_2mb.txt (2MB)")

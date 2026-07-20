#!/usr/bin/env python3
"""
FacetQL Renaming Script (v2)
Fixes underscore-boundary misses (e.g., FACETQL_TOKENS -> FACETQL_TOKENS)
"""
import os
import re
import argparse
from pathlib import Path

IGNORE_DIRS = {'.git', 'target', 'venv', 'env', 'node_modules', '__pycache__'}
IGNORE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.ico', '.lock', '.bin', '.exe', '.dll', '.so', '.dylib'}

# ORDER MATTERS: More specific patterns (with underscores) MUST come first
REPLACEMENTS = [
    # 1. Underscore prefixes/suffixes (Env vars, constants, snake_case)
    (r'FACETQL_', 'FACETQL_'),
    (r'facetql_', 'facetql_'),
    (r'FacetQL_', 'FacetQL_'),

    # 2. Exact brand matches
    (r'\bEnochianDB\b', 'FacetQL'),
    (r'\benochianDB\b', 'FacetQL'),
    (r'\bEnochian\b', 'FacetQL'),
    (r'\benochian\b', 'facetql'),
    (r'\bENOCHIAN\b', 'FACETQL'),

    # 3. CLI commands (e.g., "facetql import" -> "facetql import")
    (r'\benochian\s+([a-z]+)', r'facetql \1'),
]

def is_binary(file_path: Path) -> bool:
    try:
        with open(file_path, 'rb') as f:
            return b'\0' in f.read(1024)
    except Exception:
        return True

def process_file(file_path: Path, dry_run: bool) -> bool:
    if file_path.suffix in IGNORE_EXTENSIONS or is_binary(file_path):
        return False

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
    except UnicodeDecodeError:
        return False

    new_content = original_content
    changes_made = []

    for pattern, replacement in REPLACEMENTS:
        new_content, count = re.subn(pattern, replacement, new_content)
        if count > 0:
            changes_made.append(f"  - '{pattern}' -> '{replacement}' ({count}x)")

    if new_content != original_content:
        if dry_run:
            print(f"\n[DRY RUN] Would modify: {file_path}")
            for change in changes_made:
                print(change)
        else:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"[UPDATED] {file_path}")
        return True
    return False

def main():
    parser = argparse.ArgumentParser(description="Rename FacetQL remnants to FacetQL (v2)")
    parser.add_argument('--execute', action='store_true', help="Actually perform the changes")
    args = parser.parse_args()

    dry_run = not args.execute
    print(f"=== FacetQL Refactoring Script v2 ({'DRY RUN' if dry_run else 'EXECUTION'}) ===")

    root_dir = Path.cwd()
    files_modified = 0

    for file_path in root_dir.rglob('*'):
        if file_path.is_file() and not any(ignore in file_path.parts for ignore in IGNORE_DIRS):
            if process_file(file_path, dry_run):
                files_modified += 1

    print(f"\n=== Summary ===")
    print(f"Files modified: {files_modified}")
    if dry_run:
        print("Run with '--execute' to apply these changes.")

if __name__ == '__main__':
    main()
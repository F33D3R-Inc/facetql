#!/usr/bin/env python3
"""
FacetQL Renaming Script
Safely renames 'facetql' / 'FacetQL' references to 'facetql' / 'FacetQL'
across Rust source files, documentation, and configuration files.
"""

import os
import re
import argparse
from pathlib import Path

# Directories and files to ignore
IGNORE_DIRS = {'.git', 'target', 'venv', 'env', 'node_modules', '__pycache__'}
IGNORE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.ico', '.lock', '.bin', '.exe', '.dll', '.so', '.dylib'}

# Precise replacement mappings (Order matters: more specific first)
REPLACEMENTS = [
    # Brand names
    (r'\bEnochianDB\b', 'FacetQL'),
    (r'\benochianDB\b', 'FacetQL'),
    (r'\bENOCHIAN-LLC\b', 'FACETQL-LLC'), # Update org name if desired, or keep as is
    (r'\bEnochian\b', 'FacetQL'),
    (r'\benochian\b', 'facetql'),
    (r'\bENOCHIAN\b', 'FACETQL'),

    # Snake_case variables/paths
    (r'\benochian_db\b', 'facetql'),
    (r'\benochian_db_', 'facetql_'),

    # CLI commands (e.g., "facetql import" -> "facetql import")
    (r'\benochian\s+([a-z]+)', r'facetql \1'),
]

def is_binary(file_path: Path) -> bool:
    """Check if a file is binary by reading the first 1024 bytes."""
    try:
        with open(file_path, 'rb') as f:
            chunk = f.read(1024)
            return b'\0' in chunk
    except Exception:
        return True

def process_file(file_path: Path, dry_run: bool) -> bool:
    """Process a single file and apply replacements."""
    if file_path.suffix in IGNORE_EXTENSIONS:
        return False

    if is_binary(file_path):
        return False

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
    except UnicodeDecodeError:
        return False

    new_content = original_content
    changes_made = []

    for pattern, replacement in REPLACEMENTS:
        # Use re.IGNORECASE for the base 'facetql' to catch weird casing,
        # but our specific patterns above handle the most common cases.
        new_content, count = re.subn(pattern, replacement, new_content)
        if count > 0:
            changes_made.append(f"  - Replaced '{pattern}' -> '{replacement}' ({count} times)")

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

def rename_files_and_dirs(root_dir: Path, dry_run: bool):
    """Rename files and directories that contain 'facetql' in their name."""
    for path in root_dir.rglob('*'):
        if any(ignore in path.parts for ignore in IGNORE_DIRS):
            continue

        if 'facetql' in path.name.lower():
            new_name = path.name.lower().replace('facetql', 'facetql')
            # Handle PascalCase if it was EnochianSomething
            if 'FacetQL' in path.name:
                new_name = path.name.replace('FacetQL', 'FacetQL')
            elif 'FACETQL' in path.name:
                new_name = path.name.replace('FACETQL', 'FACETQL')
            else:
                new_name = new_name # already lowercase facetql

            if new_name != path.name:
                new_path = path.parent / new_name
                if dry_run:
                    print(f"[DRY RUN] Would rename: {path.name} -> {new_name}")
                else:
                    path.rename(new_path)
                    print(f"[RENAMED] {path.name} -> {new_name}")

def main():
    parser = argparse.ArgumentParser(description="Rename FacetQL to FacetQL")
    parser.add_argument('--execute', action='store_true', help="Actually perform the changes (default is dry-run)")
    args = parser.parse_args()

    dry_run = not args.execute
    mode = "DRY RUN" if dry_run else "EXECUTION"

    print(f"=== FacetQL Refactoring Script ({mode}) ===")
    print(f"Target Directory: {Path.cwd()}")
    if dry_run:
        print("⚠️  Running in DRY RUN mode. No files will be modified.")
        print("   Use '--execute' to apply changes.\n")

    root_dir = Path.cwd()
    files_modified = 0

    # 1. Process file contents
    print("--- Scanning file contents ---")
    for file_path in root_dir.rglob('*'):
        if file_path.is_file() and not any(ignore in file_path.parts for ignore in IGNORE_DIRS):
            if process_file(file_path, dry_run):
                files_modified += 1

    # 2. Rename files and directories
    print("\n--- Scanning file/directory names ---")
    rename_files_and_dirs(root_dir, dry_run)

    print(f"\n=== Summary ===")
    print(f"Files that would be/were modified: {files_modified}")
    if dry_run:
        print("Review the output above. If it looks correct, run: python rename_to_facetql.py --execute")

if __name__ == '__main__':
    main()
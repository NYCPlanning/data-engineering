#!/usr/bin/env python3
"""
Recalculate start_index and end_index in text formatting CSV files based on field_length.

Usage:
    python fix_text_formatting_indexes.py <csv_file>
"""

import csv
import sys
from pathlib import Path


def fix_indexes(csv_path: Path) -> None:
    """Read CSV, recalculate start/end indexes based on field_length, and save."""
    
    # Read the CSV
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames
    
    # Extract prefix from first field_number (e.g., "TL" from "TL1")
    first_field_number = rows[0]['field_number']
    prefix = ''.join(c for c in first_field_number if not c.isdigit() and c != '_')
    
    # Recalculate indexes and field numbers
    current_position = 1
    for idx, row in enumerate(rows, start=1):
        field_length = int(row['field_length'])
        
        # Auto-number field_number
        row['field_number'] = f"{prefix}{idx}"
        
        row['start_index'] = str(current_position)
        row['end_index'] = str(current_position + field_length - 1)
        
        current_position += field_length
    
    # Write back to CSV
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"✓ Fixed indexes in {csv_path}")
    print(f"  Total fields: {len(rows)}")
    print(f"  Total length: {current_position - 1}")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python fix_text_formatting_indexes.py <csv_file>")
        sys.exit(1)
    
    csv_file = Path(sys.argv[1])
    
    if not csv_file.exists():
        print(f"Error: File not found: {csv_file}")
        sys.exit(1)
    
    fix_indexes(csv_file)

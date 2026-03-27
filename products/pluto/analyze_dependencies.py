#!/usr/bin/env python3
"""
Analyze UPDATE statement dependencies in PLUTO SQL files.
Creates a dependency graph to help plan dbt migration.
"""

import re
import os
from pathlib import Path
from collections import defaultdict

def extract_update_info(sql_content, file_path):
    """Extract UPDATE statement information from SQL content."""
    updates = []
    
    # Find all UPDATE statements (case insensitive, multiline)
    # Pattern: UPDATE table_name SET ... FROM ... WHERE ...
    update_pattern = re.compile(
        r'UPDATE\s+(\w+)\s+(?:AS\s+\w+\s+)?SET\s+(.*?)(?:FROM\s+(.*?))?(?:WHERE\s+(.*?))?(?:;|UPDATE|$)',
        re.IGNORECASE | re.DOTALL
    )
    
    matches = update_pattern.finditer(sql_content)
    
    for match in matches:
        target_table = match.group(1).lower()
        set_clause = match.group(2)
        from_clause = match.group(3) if match.group(3) else ""
        where_clause = match.group(4) if match.group(4) else ""
        
        # Extract field assignments from SET clause
        # Pattern: field = value or field = source.field
        set_fields = []
        for field_match in re.finditer(r'(\w+)\s*=\s*([^,]+?)(?:,|$)', set_clause, re.DOTALL):
            field_name = field_match.group(1).strip()
            field_value = field_match.group(2).strip()
            set_fields.append((field_name, field_value))
        
        # Extract source tables from FROM clause
        source_tables = []
        if from_clause:
            # Simple extraction of table names (may need refinement)
            table_matches = re.findall(r'(\w+)\s+(?:AS\s+)?(\w+)?', from_clause, re.IGNORECASE)
            for table_name, alias in table_matches:
                if table_name.lower() not in ['left', 'right', 'inner', 'outer', 'join', 'on']:
                    source_tables.append(table_name.lower())
        
        # Extract join conditions
        join_conditions = []
        if where_clause:
            # Look for join patterns like table1.field = table2.field
            join_matches = re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', where_clause, re.IGNORECASE)
            join_conditions.extend(join_matches)
        
        updates.append({
            'file': file_path,
            'target_table': target_table,
            'fields': set_fields,
            'source_tables': source_tables,
            'join_conditions': join_conditions,
            'raw_update': match.group(0)[:200]  # First 200 chars for reference
        })
    
    return updates


def analyze_pluto_updates(base_path):
    """Analyze all SQL files in pluto_build directory."""
    all_updates = []
    
    # Find all .sql files
    sql_files = Path(base_path).rglob('*.sql')
    
    for sql_file in sorted(sql_files):
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            relative_path = sql_file.relative_to(Path(base_path).parent)
            updates = extract_update_info(content, str(relative_path))
            all_updates.extend(updates)
            
        except Exception as e:
            print(f"Error processing {sql_file}: {e}")
    
    return all_updates


def write_dependency_graph(updates, output_file):
    """Write dependency graph to file in structured format."""
    
    # Group by file
    by_file = defaultdict(list)
    for update in updates:
        by_file[update['file']].append(update)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# PLUTO UPDATE Statement Dependency Analysis\n")
        f.write("# Generated for dbt migration planning\n")
        f.write(f"# Total files analyzed: {len(by_file)}\n")
        f.write(f"# Total UPDATE statements: {len(updates)}\n\n")
        
        f.write("=" * 80 + "\n")
        f.write("SUMMARY BY TARGET TABLE\n")
        f.write("=" * 80 + "\n\n")
        
        # Group by target table
        by_target = defaultdict(list)
        for update in updates:
            by_target[update['target_table']].append(update)
        
        for table in sorted(by_target.keys()):
            updates_list = by_target[table]
            f.write(f"\n{table.upper()} ({len(updates_list)} updates)\n")
            f.write("-" * 40 + "\n")
            
            # Count fields updated
            all_fields = set()
            for upd in updates_list:
                for field_name, _ in upd['fields']:
                    all_fields.add(field_name.lower())
            
            f.write(f"Fields updated: {', '.join(sorted(all_fields))}\n")
            f.write(f"Files: {', '.join(sorted(set(Path(u['file']).name for u in updates_list)))}\n")
        
        f.write("\n\n")
        f.write("=" * 80 + "\n")
        f.write("DETAILED DEPENDENCY GRAPH\n")
        f.write("=" * 80 + "\n\n")
        
        for file_path in sorted(by_file.keys()):
            f.write(f"\n{'=' * 80}\n")
            f.write(f"FILE: {file_path}\n")
            f.write(f"{'=' * 80}\n\n")
            
            for i, update in enumerate(by_file[file_path], 1):
                f.write(f"--- UPDATE #{i} ---\n")
                f.write(f"Target Table: {update['target_table']}\n")
                f.write(f"Fields Updated:\n")
                
                for field_name, field_value in update['fields']:
                    f.write(f"  - {field_name} = {field_value[:100]}\n")
                
                if update['source_tables']:
                    f.write(f"Source Tables: {', '.join(update['source_tables'])}\n")
                
                if update['join_conditions']:
                    f.write(f"Join Conditions:\n")
                    for t1, f1, t2, f2 in update['join_conditions']:
                        f.write(f"  - {t1}.{f1} = {t2}.{f2}\n")
                
                f.write(f"\nSQL Preview:\n{update['raw_update']}\n")
                f.write("\n" + "-" * 40 + "\n\n")


def main():
    base_path = "products/pluto/pluto_build"
    output_file = "products/pluto/dependencies.txt"
    
    print(f"Analyzing SQL files in {base_path}...")
    updates = analyze_pluto_updates(base_path)
    
    print(f"Found {len(updates)} UPDATE statements")
    print(f"Writing dependency graph to {output_file}...")
    
    write_dependency_graph(updates, output_file)
    
    print("✓ Analysis complete!")
    print(f"\nNext steps:")
    print(f"1. Review {output_file}")
    print(f"2. Identify leaf nodes (fields with no downstream dependencies)")
    print(f"3. Group related updates for batch migration")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Analyze PLUTO dependencies to identify migration groups and leaf nodes.
"""

import re
from collections import defaultdict
from pathlib import Path


def parse_dependencies_file(filepath):
    """Parse the dependencies.txt file to extract update information."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    updates = []
    current_file = None
    
    # Split by file sections
    file_sections = re.split(r'={80}\nFILE: (.*?)\n={80}', content)
    
    for i in range(1, len(file_sections), 2):
        file_path = file_sections[i]
        section_content = file_sections[i + 1]
        
        # Extract update blocks
        update_blocks = re.split(r'--- UPDATE #\d+ ---', section_content)
        
        for block in update_blocks[1:]:  # Skip first empty split
            target_match = re.search(r'Target Table: (\w+)', block)
            if not target_match:
                continue
            
            target_table = target_match.group(1)
            
            # Extract fields
            fields = []
            field_section = re.search(r'Fields Updated:(.*?)(?:Source Tables:|Join Conditions:|SQL Preview:|$)', block, re.DOTALL)
            if field_section:
                field_lines = field_section.group(1).strip().split('\n')
                for line in field_lines:
                    field_match = re.match(r'\s*- (\w+) =', line)
                    if field_match:
                        fields.append(field_match.group(1))
            
            # Extract source tables
            sources = []
            source_section = re.search(r'Source Tables: (.*?)(?:\n|$)', block)
            if source_section:
                sources = [s.strip() for s in source_section.group(1).split(',')]
            
            updates.append({
                'file': Path(file_path).name,
                'target_table': target_table,
                'fields': fields,
                'sources': sources
            })
    
    return updates


def analyze_field_dependencies(updates):
    """Analyze which fields depend on other fields."""
    # Map: table.field -> list of files that update it
    field_writers = defaultdict(list)
    
    # Map: file -> fields it depends on (reads from)
    field_readers = defaultdict(set)
    
    for update in updates:
        table = update['target_table']
        for field in update['fields']:
            field_writers[f"{table}.{field}"].append(update['file'])
        
        # Sources indicate reading from those tables
        for source in update['sources']:
            field_readers[update['file']].add(source)
    
    return field_writers, field_readers


def identify_migration_groups(updates):
    """Identify logical groupings for migration."""
    
    # Group by file prefix/topic
    by_topic = defaultdict(list)
    
    for update in updates:
        file = update['file']
        
        # Categorize by filename patterns
        if 'cama_' in file:
            topic = 'CAMA'
        elif 'zoning' in file:
            topic = 'Zoning'
        elif 'geocode' in file or 'latlong' in file or file in ['dtmgeoms.sql', 'dtmmergepolygons.sql']:
            topic = 'Geocoding'
        elif 'primebbl' in file:
            topic = 'Prime BBL'
        elif file in ['bldgclass.sql', 'bsmttype.sql', 'easements.sql', 'lottype.sql', 'proxcode.sql']:
            topic = 'Classification/Defaults'
        elif 'yearbuilt' in file:
            topic = 'Year Built'
        elif file in ['backfill.sql', 'corr_lotarea.sql']:
            topic = 'Corrections/Backfill'
        elif file == 'apply_dbt_enrichments.sql':
            topic = 'DBT Integration (Already Migrated)'
        else:
            topic = 'Other'
        
        by_topic[topic].append(update)
    
    return by_topic


def main():
    deps_file = "products/pluto/dependencies.txt"
    
    print("Parsing dependencies...")
    updates = parse_dependencies_file(deps_file)
    
    print(f"\nTotal UPDATE statements analyzed: {len(updates)}")
    
    # Analyze field dependencies
    field_writers, field_readers = analyze_field_dependencies(updates)
    
    # Identify migration groups
    groups = identify_migration_groups(updates)
    
    print("\n" + "=" * 80)
    print("MIGRATION GROUPS")
    print("=" * 80)
    
    for topic in sorted(groups.keys()):
        files = set(u['file'] for u in groups[topic])
        tables = set(u['target_table'] for u in groups[topic])
        all_fields = set()
        for u in groups[topic]:
            all_fields.update(u['fields'])
        
        print(f"\n{topic}")
        print("-" * 40)
        print(f"Files ({len(files)}): {', '.join(sorted(files))}")
        print(f"Target tables: {', '.join(sorted(tables))}")
        print(f"Fields updated ({len(all_fields)}): {', '.join(sorted(all_fields)[:10])}")
        if len(all_fields) > 10:
            print(f"  ... and {len(all_fields) - 10} more")
    
    print("\n" + "=" * 80)
    print("LEAF CANDIDATES (Fields written but not read as dependencies)")
    print("=" * 80)
    print("\nNote: These fields appear to be final outputs with no downstream SQL dependencies")
    print("They are good candidates for early migration.\n")
    
    # Fields that are written to but not used as sources elsewhere
    written_tables = set(u['target_table'] for u in updates)
    source_tables = set()
    for u in updates:
        source_tables.update(u['sources'])
    
    leaf_candidates = written_tables - source_tables
    
    for table in sorted(leaf_candidates):
        updates_to_table = [u for u in updates if u['target_table'] == table]
        fields = set()
        for u in updates_to_table:
            fields.update(u['fields'])
        print(f"{table}: {', '.join(sorted(fields)[:15])}")
        if len(fields) > 15:
            print(f"  ... and {len(fields) - 15} more")
    
    print("\n" + "=" * 80)
    print("INTERMEDIATE TABLES (Read by other updates)")
    print("=" * 80)
    print("\nThese tables must be migrated before their dependents:\n")
    
    intermediate = written_tables & source_tables
    for table in sorted(intermediate):
        # Find what reads from this table
        readers = set()
        for u in updates:
            if table in u['sources']:
                readers.add(u['file'])
        
        print(f"{table}")
        print(f"  Written by: {', '.join(set(u['file'] for u in updates if u['target_table'] == table))}")
        print(f"  Read by: {', '.join(sorted(readers))}")
    
    print("\n" + "=" * 80)
    print("RECOMMENDED MIGRATION ORDER")
    print("=" * 80)
    print("""
1. PRIME BBL (foundational - sets up primary key relationships)
2. CLASSIFICATION/DEFAULTS (simple null fills and classifications)
3. CAMA (business logic for various property attributes)
4. GEOCODING (lat/long, geometry operations)
5. ZONING (complex multi-table zoning logic)
6. YEAR BUILT (allocated table operations)
7. CORRECTIONS/BACKFILL (depends on other fields being set)

Within each group, files can likely be migrated in parallel as separate dbt models.
    """)


if __name__ == "__main__":
    main()

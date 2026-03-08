import json
import logging
import time
import sys
import os
from db_interface import AS400DB2Interface
from mock_generator import MockDataGenerator

def setup_logging(log_level):
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def load_json(filepath):
    with open(filepath, 'r') as f:
        return json.load(f)

def prompt_yes_no(question):
    while True:
        choice = input(f"{question} (y/n): ").lower()
        if choice in ['y', 'yes']: return True
        if choice in ['n', 'no']: return False

def get_verified_schema(db, default_schema):
    schema = default_schema
    while True:
        if not schema:
            schema = input("Enter the Library (Schema) name to use: ").upper()
        
        if db.schema_exists(schema):
            print(f"Verified: Library '{schema}' exists.")
            return schema
        else:
            print(f"Error: Library '{schema}' not found on system.")
            if not prompt_yes_no("Try entering a different library name?"):
                return None
            schema = ""

def verify_schema(db, table_schema, active_schema):
    table_name = table_schema['table_name'].upper()
    expected_columns = {col['name'].upper(): col for col in table_schema['columns']}
    actual_columns_list = db.get_table_columns(table_name, active_schema)

    # 1. Handle Missing Table
    if not actual_columns_list:
        print(f"Error: Table {table_name} does not exist in library {active_schema}.")
        if prompt_yes_no(f"Should the table {table_name} be created in {active_schema}?"):
            if db.create_table(table_name, table_schema['columns'], active_schema):
                print(f"Table {table_name} created successfully.")
                setup_journaling(db, table_name, active_schema)
                return next((c['name'] for c in table_schema['columns'] if c.get('primary_key')), None)
            else:
                print(f"Failed to create table {table_name}.")
        return None

    actual_columns = {col['name']: col for col in actual_columns_list}
    differences = []
    for col_name, expected_info in expected_columns.items():
        if col_name not in actual_columns:
            differences.append(f"Missing column: {col_name}")
            continue
        actual_info = actual_columns[col_name]
        expected_type = expected_info['type'].upper()
        expected_type_base = expected_type.split('(')[0] if '(' in expected_type else expected_type
        if expected_type_base not in actual_info['type'].upper():
             differences.append(f"Type mismatch for {col_name}: Expected {expected_type}, got {actual_info['type']}")

    # 2. Handle Schema Mismatch
    if differences:
        print(f"Schema mismatch for {table_name} in {active_schema}:")
        for diff in differences: print(f"  - {diff}")
        print(f"\\nExisting structure in Database:")
        for col_name, info in actual_columns.items():
            print(f"  {col_name}: {info['type']} (Length: {info['length']}, Precision: {info['precision']}, Scale: {info['scale']})")
        choice = input(f"Continue (c), Drop and Recreate (d), or Skip (s)? ").lower()
        if choice == 'd':
            if db.drop_table(table_name, active_schema) and db.create_table(table_name, table_schema['columns'], active_schema):
                print(f"Table {table_name} recreated successfully.")
                setup_journaling(db, table_name, active_schema)
                return next((c['name'] for c in table_schema['columns'] if c.get('primary_key')), None)
        elif choice == 'c':
            return next((c['name'] for c in table_schema['columns'] if c.get('primary_key')), None)
        return None
    
    print(f"Schema verification passed for {table_name}.")
    return next((c['name'] for c in table_schema['columns'] if c.get('primary_key')), None)

def setup_journaling(db, table_name, active_schema):
    lib_j_info = db.check_library_journaling(active_schema)
    if lib_j_info:
        print(f"Default journaling detected in library: {lib_j_info['journal_library']}/{lib_j_info['journal_name']}")
        j_info = db.get_journal_info(table_name, active_schema)
        if j_info:
            print(f"Table {table_name} is automatically journaled: {j_info['journal_library']}/{j_info['journal_name']}")
            return
        if prompt_yes_no(f"Start journaling table {table_name} using library default?"):
            if db.start_journaling(table_name, lib_j_info['journal_library'], lib_j_info['journal_name'], active_schema):
                print("Journaling started successfully.")
            return

    if prompt_yes_no(f"Table {table_name} is not journaled. Start journaling?"):
        j_lib = input(f"Enter Journal Library (default {active_schema}): ") or active_schema
        j_name = input(f"Enter Journal Name: ")
        if db.start_journaling(table_name, j_lib, j_name, active_schema):
            print("Journaling started successfully.")

def process_table(db, gen, table_schema, profile, batch_size, active_schema):
    table_name = table_schema['table_name']
    print(f"\\nProcessing table: {table_name}")
    pk_col = verify_schema(db, table_schema, active_schema)
    if not pk_col: return False

    j_info = db.get_journal_info(table_name, active_schema)
    if j_info:
        print(f"Journaling info: Library={j_info['journal_library']}, Name={j_info['journal_name']}, Receiver={j_info['receiver_name']}")
        je_info = db.get_journal_entries_info(table_name, active_schema)
        if je_info: print(f"Oldest Seq: {je_info.get('oldest_sequence')}, Newest Seq: {je_info.get('newest_sequence')}")

    total_records = profile.get('total_records', 0)
    ratio = profile.get('transaction_ratio', '100:0:0').split(':')
    i_count, u_count, d_count = [int(total_records * int(p) / 100) for p in ratio]
    print(f"Planned transactions: {i_count} Inserts, {u_count} Updates, {d_count} Deletes")
    
    start_time = time.time()
    results = {"success": 0, "failed": 0}
    cols = list(profile['field_mapping'].keys())

    # Inserts
    for i in range(0, i_count, batch_size):
        batch_data = [[gen.generate_record(profile['field_mapping'])[c] for c in cols] for _ in range(min(batch_size, i_count - i))]
        s, f = db.execute_bulk_insert(table_name, cols, batch_data, active_schema)
        results["success"] += s; results["failed"] += f

    # Updates
    if u_count > 0:
        pks = db.get_random_pks(table_name, pk_col, u_count, active_schema)
        if pks:
            for i in range(0, len(pks), batch_size):
                batch_pks = pks[i : i + batch_size]
                batch_data = []
                for pk in batch_pks:
                    record = gen.generate_record(profile['field_mapping'])
                    record[pk_col] = pk
                    batch_data.append([record[c] for c in cols])
                s, f = db.execute_bulk_update(table_name, cols, batch_data, pk_col, active_schema)
                results["success"] += s; results["failed"] += f

    # Deletes
    if d_count > 0:
        pks = db.get_random_pks(table_name, pk_col, d_count, active_schema)
        if pks:
            for i in range(0, len(pks), batch_size):
                s, f = db.execute_bulk_delete(table_name, pks[i : i + batch_size], pk_col, active_schema)
                results["success"] += s; results["failed"] += f

    print(f"Results for {table_name}: Success={results['success']}, Failed={results['failed']}, Time={time.time() - start_time:.2f}s")
    return True

def main():
    if not os.path.exists('config.json'):
        print("config.json not found"); sys.exit(1)
    config = load_json('config.json')
    setup_logging(config['settings'].get('log_level', 'INFO'))
    schema_data = load_json(config['files']['schema_file'])
    mockup_data = load_json(config['files']['mockup_profile'])
    db = AS400DB2Interface(config)
    gen = MockDataGenerator()
    try:
        db.connect()
        # Verify and set the active library
        active_schema = get_verified_schema(db, config['database'].get('schema', ''))
        if not active_schema:
            print("No valid library selected. Exiting."); return

        profiles = {p['table_name']: p for p in mockup_data['profiles']}
        for table_schema in schema_data['tables']:
            if table_schema['table_name'] in profiles:
                process_table(db, gen, table_schema, profiles[table_schema['table_name']], config['settings'].get('batch_size', 1000), active_schema)
    finally:
        db.close()

if __name__ == "__main__":
    main()

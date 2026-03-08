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

def get_verified_schema(db, schema):
    if not schema:
        return ""
    if db.schema_exists(schema):
        return schema.upper()
    else:
        print(f"Error: Schema {schema} does not exist.")
        sys.exit(1)

def verify_schema(db, table_schema, schema=None):
    table_name = table_schema['table_name']
    expected_columns = {col['name'].upper(): col for col in table_schema['columns']}

    actual_columns_list = db.get_table_columns(table_name, schema=schema)
    if not actual_columns_list:
        print(f"Error: Table {table_name} does not exist.")
        sys.exit(1)

    actual_columns = {col['name'].upper(): col for col in actual_columns_list}

    differences = []
    for col_name, expected_info in expected_columns.items():
        if col_name not in actual_columns:
            differences.append(f"Missing column: {col_name}")
            continue

        actual_info = actual_columns[col_name]
        # Basic type comparison (e.g., 'INTEGER' vs 'INTEGER', 'VARCHAR(50)' vs 'VARCHAR')
        expected_type = expected_info['type'].upper()
        if '(' in expected_type:
            expected_type_base = expected_type.split('(')[0]
        else:
            expected_type_base = expected_type

        if expected_type_base not in actual_info['type'].upper():
             differences.append(f"Type mismatch for {col_name}: Expected {expected_type}, got {actual_info['type']}")

    if differences:
        print(f"Schema mismatch for {table_name}:")
        for diff in differences:
            print(f"  - {diff}")
        print("Exiting due to schema mismatch.")
        sys.exit(1)
    
    print(f"Schema verification passed for {table_name}.")
    pk_col = next((col['name'] for col in table_schema['columns'] if col.get('primary_key')), None)
    return pk_col

def process_table(db, gen, table_schema, profile, batch_size, schema=None):
    table_name = table_schema['table_name']
    print(f"\nProcessing table: {table_name}")

    # 1. Verify schema - will exit if mismatch
    pk_col = verify_schema(db, table_schema, schema=schema)

    # 2. Journaling info
    j_info = db.get_journal_info(table_name, schema=schema)
    if j_info:
        print(f"Journaling enabled: Library={j_info['journal_library']}, Name={j_info['journal_name']}, Receiver={j_info['receiver_name']}")
        # 3. Journal entries info
        je_info = db.get_journal_entries_info(table_name, schema=schema)
        if je_info:
            print(f"Oldest Sequence: {je_info.get('oldest_sequence')}")
            print(f"Newest Sequence: {je_info.get('newest_sequence')}")
    else:
        print("Journaling not enabled or not found.")

    # 4. Generate and execute transactions
    total_records = profile.get('total_records', 0)
    ratio = profile.get('transaction_ratio', '100:0:0').split(':')
    i_pct, u_pct, d_pct = [int(p) for p in ratio]

    i_count = int(total_records * i_pct / 100)
    u_count = int(total_records * u_pct / 100)
    d_count = int(total_records * d_pct / 100)

    print(f"Planned transactions: {i_count} Inserts, {u_count} Updates, {d_count} Deletes")
    
    start_time = time.time()
    results = {"success": 0, "failed": 0}

    # Inserts
    for i in range(0, i_count, batch_size):
        current_batch_size = min(batch_size, i_count - i)
        batch_data = []
        cols = list(profile['field_mapping'].keys())
        for _ in range(current_batch_size):
            record = gen.generate_record(profile['field_mapping'])
            batch_data.append([record[c] for c in cols])

        s, f = db.execute_bulk_insert(table_name, cols, batch_data)
        results["success"] += s
        results["failed"] += f

    # Updates
    if u_count > 0 and pk_col:
        pks = db.get_random_pks(table_name, pk_col, u_count)
        if pks:
            cols = list(profile['field_mapping'].keys())
            if pk_col not in cols:
                cols.append(pk_col)

            for i in range(0, len(pks), batch_size):
                current_batch_pks = pks[i : i + batch_size]
                batch_data = []
                for pk in current_batch_pks:
                    record = gen.generate_record(profile['field_mapping'])
                    record[pk_col] = pk
                    batch_data.append([record[c] for c in cols])

                s, f = db.execute_bulk_update(table_name, cols, batch_data, pk_col)
                results["success"] += s
                results["failed"] += f
        else:
            print(f"No records found in {table_name} to update.")

    # Deletes
    if d_count > 0 and pk_col:
        pks = db.get_random_pks(table_name, pk_col, d_count)
        if pks:
            for i in range(0, len(pks), batch_size):
                current_batch_pks = pks[i : i + batch_size]
                s, f = db.execute_bulk_delete(table_name, current_batch_pks, pk_col)
                results["success"] += s
                results["failed"] += f
        else:
            print(f"No records found in {table_name} to delete.")

    end_time = time.time()
    print(f"Results for {table_name}: Success={results['success']}, Failed={results['failed']}, Time={end_time - start_time:.2f}s")
    return True

def main():
    if not os.path.exists('config.json'):
        print("config.json not found")
        sys.exit(1)

    config = load_json('config.json')
    setup_logging(config['settings'].get('log_level', 'INFO'))

    schema_data = load_json(config['files']['schema_file'])
    mockup_data = load_json(config['files']['mockup_profile'])

    db = AS400DB2Interface(config)
    gen = MockDataGenerator()

    try:
        db.connect()
        active_schema = get_verified_schema(db, config['database'].get('schema', ''))
        profiles = {p['table_name']: p for p in mockup_data['profiles']}

        for table_schema in schema_data['tables']:
            table_name = table_schema['table_name']
            if table_name in profiles:
                process_table(db, gen, table_schema, profiles[table_name], config['settings'].get('batch_size', 1000), schema=active_schema)
            else:
                print(f"No mockup profile found for {table_name}, skipping.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        db.close()

if __name__ == "__main__":
    main()

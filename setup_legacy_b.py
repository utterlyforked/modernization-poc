"""Setup Legacy System B - Multi-tenant instances"""
import sqlite3

def setup_legacy_b_instance(instance_name, tenant_id):
    db_name = f'legacy_b_{instance_name}.db'
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create person table for Legacy B
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS person (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firstname TEXT NOT NULL,
        surname TEXT NOT NULL,
        date_of_birth TEXT NOT NULL,
        city TEXT NOT NULL,
        data_a TEXT,
        data_b TEXT,
        data_c TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create change log table for CDC
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS person_changelog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        person_id INTEGER NOT NULL,
        operation TEXT NOT NULL,
        firstname TEXT,
        surname TEXT,
        date_of_birth TEXT,
        city TEXT,
        data_a TEXT,
        data_b TEXT,
        data_c TEXT,
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed INTEGER DEFAULT 0
    )
    ''')
    
    # CDC Trigger for INSERT
    cursor.execute('''
    CREATE TRIGGER IF NOT EXISTS person_insert_trigger
    AFTER INSERT ON person
    BEGIN
        INSERT INTO person_changelog (person_id, operation, firstname, surname, date_of_birth, city, data_a, data_b, data_c)
        VALUES (NEW.id, 'INSERT', NEW.firstname, NEW.surname, NEW.date_of_birth, NEW.city, NEW.data_a, NEW.data_b, NEW.data_c);
    END
    ''')
    
    # CDC Trigger for UPDATE
    cursor.execute('''
    CREATE TRIGGER IF NOT EXISTS person_update_trigger
    AFTER UPDATE ON person
    BEGIN
        INSERT INTO person_changelog (person_id, operation, firstname, surname, date_of_birth, city, data_a, data_b, data_c)
        VALUES (NEW.id, 'UPDATE', NEW.firstname, NEW.surname, NEW.date_of_birth, NEW.city, NEW.data_a, NEW.data_b, NEW.data_c);
    END
    ''')
    
    # Insert sample data specific to this tenant
    if instance_name == 'tenant1':
        cursor.execute('''
        INSERT INTO person (firstname, surname, date_of_birth, city, data_a, data_b, data_c)
        VALUES ('Diana', 'Prince', '1988-11-05', 'Seattle', 'delta_a', 'delta_b', 'delta_c')
        ''')
        cursor.execute('''
        INSERT INTO person (firstname, surname, date_of_birth, city, data_a, data_b, data_c)
        VALUES ('Ethan', 'Hunt', '1987-07-20', 'Miami', 'epsilon_a', 'epsilon_b', 'epsilon_c')
        ''')
    else:
        cursor.execute('''
        INSERT INTO person (firstname, surname, date_of_birth, city, data_a, data_b, data_c)
        VALUES ('Fiona', 'Green', '1995-01-12', 'Portland', 'zeta_a', 'zeta_b', 'zeta_c')
        ''')
    
    conn.commit()
    conn.close()
    print(f"✅ Legacy B - {instance_name} ({tenant_id}) setup complete")

if __name__ == '__main__':
    setup_legacy_b_instance('tenant1', 'tenant_b1')
    setup_legacy_b_instance('tenant2', 'tenant_b2')

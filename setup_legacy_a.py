"""Setup Legacy System A - Multi-tenant instances"""
import sqlite3

def setup_legacy_a_instance(instance_name, tenant_id):
    db_name = f'legacy_a_{instance_name}.db'
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create person table for Legacy A
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS person (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firstname TEXT NOT NULL,
        surname TEXT NOT NULL,
        date_of_birth TEXT NOT NULL,
        city TEXT NOT NULL,
        data_1 TEXT,
        data_2 TEXT,
        data_3 TEXT,
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
        data_1 TEXT,
        data_2 TEXT,
        data_3 TEXT,
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed INTEGER DEFAULT 0
    )
    ''')
    
    # CDC Trigger for INSERT
    cursor.execute('''
    CREATE TRIGGER IF NOT EXISTS person_insert_trigger
    AFTER INSERT ON person
    BEGIN
        INSERT INTO person_changelog (person_id, operation, firstname, surname, date_of_birth, city, data_1, data_2, data_3)
        VALUES (NEW.id, 'INSERT', NEW.firstname, NEW.surname, NEW.date_of_birth, NEW.city, NEW.data_1, NEW.data_2, NEW.data_3);
    END
    ''')
    
    # CDC Trigger for UPDATE
    cursor.execute('''
    CREATE TRIGGER IF NOT EXISTS person_update_trigger
    AFTER UPDATE ON person
    BEGIN
        INSERT INTO person_changelog (person_id, operation, firstname, surname, date_of_birth, city, data_1, data_2, data_3)
        VALUES (NEW.id, 'UPDATE', NEW.firstname, NEW.surname, NEW.date_of_birth, NEW.city, NEW.data_1, NEW.data_2, NEW.data_3);
    END
    ''')
    
    # Insert sample data specific to this tenant
    if instance_name == 'tenant1':
        cursor.execute('''
        INSERT INTO person (firstname, surname, date_of_birth, city, data_1, data_2, data_3)
        VALUES ('Alice', 'Smith', '1990-05-15', 'New York', 'alpha_1', 'alpha_2', 'alpha_3')
        ''')
        cursor.execute('''
        INSERT INTO person (firstname, surname, date_of_birth, city, data_1, data_2, data_3)
        VALUES ('Bob', 'Johnson', '1985-08-22', 'Boston', 'beta_1', 'beta_2', 'beta_3')
        ''')
    else:
        cursor.execute('''
        INSERT INTO person (firstname, surname, date_of_birth, city, data_1, data_2, data_3)
        VALUES ('Carol', 'Davis', '1992-03-10', 'Chicago', 'gamma_1', 'gamma_2', 'gamma_3')
        ''')
    
    conn.commit()
    conn.close()
    print(f"✅ Legacy A - {instance_name} ({tenant_id}) setup complete")

if __name__ == '__main__':
    setup_legacy_a_instance('tenant1', 'tenant_a1')
    setup_legacy_a_instance('tenant2', 'tenant_a2')

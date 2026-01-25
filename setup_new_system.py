"""Setup New Multi-Tenant System database"""
import sqlite3

def setup_new_system():
    conn = sqlite3.connect('new_system.db')
    cursor = conn.cursor()
    
    # Create unified person table with tenant_id
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS person (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tenant_id TEXT NOT NULL,
        legacy_id INTEGER NOT NULL,
        firstname TEXT NOT NULL,
        surname TEXT NOT NULL,
        date_of_birth TEXT NOT NULL,
        city TEXT NOT NULL,
        extra_field TEXT,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        modernized_only DATE,
        UNIQUE(tenant_id, legacy_id)
    )
    ''')
    
    # Create tenant mapping table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tenant_mapping (
        tenant_id TEXT PRIMARY KEY,
        legacy_system TEXT NOT NULL,
        legacy_instance TEXT NOT NULL,
        description TEXT
    )
    ''')
    
    # Insert tenant mappings for all 4 legacy instances
    mappings = [
        ('tenant_a1', 'legacy_a', 'tenant1', 'Legacy System A - Instance 1'),
        ('tenant_a2', 'legacy_a', 'tenant2', 'Legacy System A - Instance 2'),
        ('tenant_b1', 'legacy_b', 'tenant1', 'Legacy System B - Instance 1'),
        ('tenant_b2', 'legacy_b', 'tenant2', 'Legacy System B - Instance 2'),
    ]
    
    for mapping in mappings:
        cursor.execute('''
        INSERT OR REPLACE INTO tenant_mapping (tenant_id, legacy_system, legacy_instance, description)
        VALUES (?, ?, ?, ?)
        ''', mapping)
    
    conn.commit()
    conn.close()
    print("New System database setup complete")
    print("   Tenant mappings: tenant_a1, tenant_a2, tenant_b1, tenant_b2")

if __name__ == '__main__':
    setup_new_system()

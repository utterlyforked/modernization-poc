"""CDC Sync Adapter - Processes changelog from all legacy instances"""
import sqlite3
import time
from datetime import datetime

class SyncAdapter:
    def __init__(self):
        # Connect to all 4 legacy instances
        self.legacy_connections = {
            'tenant_a1': ('legacy_a_tenant1.db', 'data_2'),
            'tenant_a2': ('legacy_a_tenant2.db', 'data_2'),
            'tenant_b1': ('legacy_b_tenant1.db', 'data_c'),
            'tenant_b2': ('legacy_b_tenant2.db', 'data_c'),
        }
        self.new_system_conn = sqlite3.connect('new_system.db')
        
    def map_to_unified(self, record, tenant_id, field_mapping):
        """Map any legacy schema to unified schema"""
        # Field mapping tells us which field to extract
        # For Legacy A: field_mapping = 'data_2' (index 8)
        # For Legacy B: field_mapping = 'data_c' (index 9)
        
        extra_field_idx = 8 if field_mapping == 'data_2' else 9
        
        return {
            'tenant_id': tenant_id,
            'legacy_id': record[1],  # person_id
            'firstname': record[3],
            'surname': record[4],
            'date_of_birth': record[5],
            'city': record[6],
            'extra_field': record[extra_field_idx]
        }
    
    def sync_instance(self, tenant_id, db_path, field_mapping):
        """Sync unprocessed changes from a specific legacy instance"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM person_changelog WHERE processed = 0')
        changes = cursor.fetchall()
        
        synced = 0
        for change in changes:
            mapped = self.map_to_unified(change, tenant_id, field_mapping)
            self.upsert_to_new_system(mapped)
            
            # Mark as processed
            cursor.execute('UPDATE person_changelog SET processed = 1 WHERE id = ?', (change[0],))
            synced += 1
        
        conn.commit()
        conn.close()
        return synced
    
    def upsert_to_new_system(self, data):
        """Insert or update person in new system"""
        cursor = self.new_system_conn.cursor()
        cursor.execute('''
        INSERT INTO person (tenant_id, legacy_id, firstname, surname, date_of_birth, city, extra_field)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tenant_id, legacy_id) DO UPDATE SET
            firstname = excluded.firstname,
            surname = excluded.surname,
            date_of_birth = excluded.date_of_birth,
            city = excluded.city,
            extra_field = excluded.extra_field,
            synced_at = CURRENT_TIMESTAMP
        ''', (
            data['tenant_id'],
            data['legacy_id'],
            data['firstname'],
            data['surname'],
            data['date_of_birth'],
            data['city'],
            data['extra_field']
        ))
        self.new_system_conn.commit()
    
    def run_sync_loop(self):
        """Run continuous sync loop for all instances"""
        print("Starting sync adapter for all 4 legacy instances...")
        print("   - tenant_a1 (Legacy A - Instance 1)")
        print("   - tenant_a2 (Legacy A - Instance 2)")
        print("   - tenant_b1 (Legacy B - Instance 1)")
        print("   - tenant_b2 (Legacy B - Instance 2)")
        print()
        
        while True:
            total_synced = 0
            sync_details = []
            
            for tenant_id, (db_path, field_mapping) in self.legacy_connections.items():
                synced = self.sync_instance(tenant_id, db_path, field_mapping)
                if synced > 0:
                    total_synced += synced
                    sync_details.append(f"{tenant_id}: {synced}")
            
            if total_synced > 0:
                details = ", ".join(sync_details)
                print(f"Synced {total_synced} records ({details}) at {datetime.now().strftime('%H:%M:%S')}")
            
            time.sleep(2)  # Poll every 2 seconds
    
    def close(self):
        self.new_system_conn.close()

if __name__ == '__main__':
    adapter = SyncAdapter()
    try:
        adapter.run_sync_loop()
    except KeyboardInterrupt:
        print("\n🛑 Sync adapter stopped")
        adapter.close()

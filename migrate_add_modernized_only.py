"""Migration script to add modernized_only column to new_system.db"""
import sqlite3

conn = sqlite3.connect('new_system.db')
cursor = conn.cursor()

try:
    cursor.execute('ALTER TABLE person ADD COLUMN modernized_only DATE')
    conn.commit()
    print("✓ Added modernized_only column to person table")
except sqlite3.OperationalError as e:
    if "duplicate column" in str(e).lower():
        print("✓ Column modernized_only already exists")
    else:
        raise

conn.close()

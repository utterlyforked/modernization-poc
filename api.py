"""Flask API - New System with Write-Through to all Legacy Instances"""
from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Tenant routing configuration
TENANT_CONFIG = {
    'tenant_a1': {'db': 'legacy_a_tenant1.db', 'type': 'legacy_a'},
    'tenant_a2': {'db': 'legacy_a_tenant2.db', 'type': 'legacy_a'},
    'tenant_b1': {'db': 'legacy_b_tenant1.db', 'type': 'legacy_b'},
    'tenant_b2': {'db': 'legacy_b_tenant2.db', 'type': 'legacy_b'},
}

class WriteThrough:
    """Transparent SQL proxy to legacy systems"""
    
    @staticmethod
    def write_to_legacy(tenant_id, data):
        """Write to the correct legacy instance based on tenant_id"""
        if tenant_id not in TENANT_CONFIG:
            raise ValueError(f"Unknown tenant: {tenant_id}")
        
        config = TENANT_CONFIG[tenant_id]
        conn = sqlite3.connect(config['db'])
        cursor = conn.cursor()
        
        if config['type'] == 'legacy_a':
            # Legacy A schema
            cursor.execute('''
            INSERT INTO person (firstname, surname, date_of_birth, city, data_1, data_2, data_3)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                data['firstname'],
                data['surname'],
                data['date_of_birth'],
                data['city'],
                data.get('data_1', ''),
                data.get('extra_field', ''),  # Map to data_2
                data.get('data_3', '')
            ))
        else:
            # Legacy B schema
            cursor.execute('''
            INSERT INTO person (firstname, surname, date_of_birth, city, data_a, data_b, data_c)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                data['firstname'],
                data['surname'],
                data['date_of_birth'],
                data['city'],
                data.get('data_a', ''),
                data.get('data_b', ''),
                data.get('extra_field', '')  # Map to data_c
            ))
        
        conn.commit()
        legacy_id = cursor.lastrowid
        conn.close()
        return legacy_id

    @staticmethod
    def update_to_legacy(tenant_id, legacy_id, data):
        """Update existing record in the correct legacy instance based on tenant_id"""
        if tenant_id not in TENANT_CONFIG:
            raise ValueError(f"Unknown tenant: {tenant_id}")

        config = TENANT_CONFIG[tenant_id]
        conn = sqlite3.connect(config['db'])
        cursor = conn.cursor()

        if config['type'] == 'legacy_a':
            # Legacy A schema
            cursor.execute('''
            UPDATE person
            SET firstname=?, surname=?, date_of_birth=?, city=?, data_2=?
            WHERE id=?
            ''', (
                data['firstname'],
                data['surname'],
                data['date_of_birth'],
                data['city'],
                data.get('extra_field', ''),
                legacy_id
            ))
        else:
            # Legacy B schema
            cursor.execute('''
            UPDATE person
            SET firstname=?, surname=?, date_of_birth=?, city=?, data_c=?
            WHERE id=?
            ''', (
                data['firstname'],
                data['surname'],
                data['date_of_birth'],
                data['city'],
                data.get('extra_field', ''),
                legacy_id
            ))

        conn.commit()
        rows_updated = cursor.rowcount
        conn.close()
        return rows_updated

@app.route('/api/persons', methods=['GET'])
def get_persons():
    """Read from new system database (local reads)"""
    tenant_id = request.args.get('tenant_id')
    
    conn = sqlite3.connect('new_system.db')
    cursor = conn.cursor()
    
    if tenant_id:
        cursor.execute('SELECT * FROM person WHERE tenant_id = ? ORDER BY id DESC', (tenant_id,))
    else:
        cursor.execute('SELECT * FROM person ORDER BY id DESC')
    
    persons = cursor.fetchall()
    conn.close()
    
    result = []
    for p in persons:
        result.append({
            'id': p[0],
            'tenant_id': p[1],
            'legacy_id': p[2],
            'firstname': p[3],
            'surname': p[4],
            'date_of_birth': p[5],
            'city': p[6],
            'extra_field': p[7],
            'synced_at': p[8],
            'modernized_only': p[9]
        })
    
    return jsonify(result)

@app.route('/api/persons', methods=['POST'])
def create_person():
    """Write through to legacy system (synchronous SQL)"""
    data = request.json
    tenant_id = data.get('tenant_id')
    
    if not tenant_id:
        return jsonify({'error': 'tenant_id required'}), 400
    
    try:
        legacy_id = WriteThrough.write_to_legacy(tenant_id, data)

        # Insert modernized_only directly into new_system.db (not in legacy)
        # CDC will sync the legacy fields, and our ON CONFLICT will preserve modernized_only
        if 'modernized_only' in data and data['modernized_only']:
            new_conn = sqlite3.connect('new_system.db')
            new_cursor = new_conn.cursor()
            # Insert with all required fields; CDC will update other fields when it syncs
            new_cursor.execute('''
                INSERT INTO person (tenant_id, legacy_id, firstname, surname, date_of_birth, city, extra_field, modernized_only)
                VALUES (?, ?, '', '', '', '', '', ?)
                ON CONFLICT(tenant_id, legacy_id) DO UPDATE SET
                    modernized_only = excluded.modernized_only
            ''', (tenant_id, legacy_id, data.get('modernized_only')))
            new_conn.commit()
            new_conn.close()

        return jsonify({
            'message': 'Person created in legacy system',
            'tenant_id': tenant_id,
            'legacy_id': legacy_id,
            'note': 'CDC will sync to new system shortly'
        }), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/persons', methods=['PUT'])
def update_person():
    """Update existing person through write-through to legacy system"""
    data = request.json
    tenant_id = data.get('tenant_id')
    legacy_id = data.get('legacy_id')

    if not tenant_id:
        return jsonify({'error': 'tenant_id required'}), 400
    if not legacy_id:
        return jsonify({'error': 'legacy_id required'}), 400

    try:
        rows_updated = WriteThrough.update_to_legacy(tenant_id, legacy_id, data)

        if rows_updated == 0:
            return jsonify({'error': 'Record not found'}), 404

        # Update modernized_only directly in new_system.db (not in legacy)
        if 'modernized_only' in data:
            new_conn = sqlite3.connect('new_system.db')
            new_cursor = new_conn.cursor()
            new_cursor.execute('''
                UPDATE person SET modernized_only = ?
                WHERE tenant_id = ? AND legacy_id = ?
            ''', (data.get('modernized_only'), tenant_id, legacy_id))
            new_conn.commit()
            new_conn.close()

        return jsonify({
            'message': 'Person updated in legacy system',
            'tenant_id': tenant_id,
            'legacy_id': legacy_id,
            'note': 'CDC will sync to new system shortly'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/legacy/<tenant_id>/persons', methods=['GET'])
def get_legacy_persons(tenant_id):
    """Direct read from legacy instance (for demo purposes)"""
    if tenant_id not in TENANT_CONFIG:
        return jsonify({'error': 'Invalid tenant_id'}), 400
    
    config = TENANT_CONFIG[tenant_id]
    conn = sqlite3.connect(config['db'])
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM person ORDER BY id DESC')
    persons = cursor.fetchall()
    conn.close()
    
    result = []
    for p in persons:
        if config['type'] == 'legacy_a':
            result.append({
                'id': p[0],
                'firstname': p[1],
                'surname': p[2],
                'date_of_birth': p[3],
                'city': p[4],
                'data_1': p[5],
                'data_2': p[6],
                'data_3': p[7]
            })
        else:
            result.append({
                'id': p[0],
                'firstname': p[1],
                'surname': p[2],
                'date_of_birth': p[3],
                'city': p[4],
                'data_a': p[5],
                'data_b': p[6],
                'data_c': p[7]
            })
    
    return jsonify(result)

@app.route('/api/legacy/<tenant_id>/persons/<int:legacy_id>', methods=['GET'])
def get_legacy_person(tenant_id, legacy_id):
    """Get single person from legacy database"""
    if tenant_id not in TENANT_CONFIG:
        return jsonify({'error': 'Invalid tenant_id'}), 400

    config = TENANT_CONFIG[tenant_id]
    conn = sqlite3.connect(config['db'])
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM person WHERE id = ?', (legacy_id,))
    person = cursor.fetchone()
    conn.close()

    if not person:
        return jsonify({'error': 'Record not found'}), 404

    # Return based on schema type
    if config['type'] == 'legacy_a':
        return jsonify({
            'id': person[0],
            'firstname': person[1],
            'surname': person[2],
            'date_of_birth': person[3],
            'city': person[4],
            'data_1': person[5],
            'data_2': person[6],
            'data_3': person[7]
        })
    else:
        return jsonify({
            'id': person[0],
            'firstname': person[1],
            'surname': person[2],
            'date_of_birth': person[3],
            'city': person[4],
            'data_a': person[5],
            'data_b': person[6],
            'data_c': person[7]
        })

@app.route('/api/legacy/<tenant_id>/persons/<int:legacy_id>', methods=['PUT'])
def update_legacy_person(tenant_id, legacy_id):
    """Update person directly in legacy database"""
    data = request.json

    if tenant_id not in TENANT_CONFIG:
        return jsonify({'error': 'Invalid tenant_id'}), 400

    config = TENANT_CONFIG[tenant_id]
    conn = sqlite3.connect(config['db'])
    cursor = conn.cursor()

    if config['type'] == 'legacy_a':
        # Update ALL Legacy A fields
        cursor.execute('''
        UPDATE person SET
            firstname=?, surname=?, date_of_birth=?, city=?,
            data_1=?, data_2=?, data_3=?
        WHERE id=?
        ''', (
            data['firstname'], data['surname'],
            data['date_of_birth'], data['city'],
            data.get('data_1', ''), data.get('data_2', ''),
            data.get('data_3', ''), legacy_id
        ))
    else:
        # Update ALL Legacy B fields
        cursor.execute('''
        UPDATE person SET
            firstname=?, surname=?, date_of_birth=?, city=?,
            data_a=?, data_b=?, data_c=?
        WHERE id=?
        ''', (
            data['firstname'], data['surname'],
            data['date_of_birth'], data['city'],
            data.get('data_a', ''), data.get('data_b', ''),
            data.get('data_c', ''), legacy_id
        ))

    conn.commit()
    rows_updated = cursor.rowcount
    conn.close()

    if rows_updated == 0:
        return jsonify({'error': 'Record not found'}), 404

    return jsonify({
        'message': 'Legacy record updated',
        'note': 'CDC will sync to new system'
    }), 200

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'tenants': list(TENANT_CONFIG.keys())
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

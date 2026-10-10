#!/usr/bin/env python3
"""
API Service - Handles reads from new system and write-through to legacy systems
"""

import json
import os
import threading
import time
import urllib.request
from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

from pipeline_events import EventWriter

app = Flask(__name__)
CORS(app)

# Database configurations
NEW_SYSTEM_DB = {
    'host': os.getenv('NEW_SYSTEM_DB_HOST', 'postgres-new-system'),
    'port': os.getenv('NEW_SYSTEM_DB_PORT', '5432'),
    'dbname': os.getenv('NEW_SYSTEM_DB_NAME', 'new_system'),
    'user': os.getenv('NEW_SYSTEM_DB_USER', 'newuser'),
    'password': os.getenv('NEW_SYSTEM_DB_PASS', 'newpass')
}

DEBEZIUM_URL = os.getenv('DEBEZIUM_URL', 'http://debezium:8083')
CONNECTOR_POLL_INTERVAL = 3  # seconds between connector state checks

events = EventWriter(NEW_SYSTEM_DB, 'api')

LEGACY_DB_CONFIG = {
    'tenant_a1': {
        'host': 'postgres-legacy-a1',
        'port': '5432',
        'dbname': 'legacy_a1',
        'user': 'legacyuser',
        'password': 'legacypass',
        'type': 'legacy_a'
    },
    'tenant_a2': {
        'host': 'postgres-legacy-a2',
        'port': '5432',
        'dbname': 'legacy_a2',
        'user': 'legacyuser',
        'password': 'legacypass',
        'type': 'legacy_a'
    },
    'tenant_b1': {
        'host': 'postgres-legacy-b1',
        'port': '5432',
        'dbname': 'legacy_b1',
        'user': 'legacyuser',
        'password': 'legacypass',
        'type': 'legacy_b'
    },
    'tenant_b2': {
        'host': 'postgres-legacy-b2',
        'port': '5432',
        'dbname': 'legacy_b2',
        'user': 'legacyuser',
        'password': 'legacypass',
        'type': 'legacy_b'
    }
}


def get_new_system_conn():
    """Get connection to new system database"""
    return psycopg2.connect(**NEW_SYSTEM_DB, cursor_factory=RealDictCursor)


def emit(*args, **kwargs):
    """Record a pipeline event for the UI. Best effort: must never affect a request."""
    try:
        events.emit(*args, **kwargs)
    except Exception as e:
        print(f"pipeline event not written: {e}", flush=True)


def get_legacy_conn(tenant_id):
    """Get connection to legacy database for given tenant"""
    config = LEGACY_DB_CONFIG.get(tenant_id)
    if not config:
        return None
    return psycopg2.connect(
        host=config['host'],
        port=config['port'],
        dbname=config['dbname'],
        user=config['user'],
        password=config['password'],
        cursor_factory=RealDictCursor
    )


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'tenants': list(LEGACY_DB_CONFIG.keys())
    })


@app.route('/api/persons', methods=['GET'])
def get_persons():
    """Get all persons from new system (local read)"""
    try:
        conn = get_new_system_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, tenant_id, legacy_id, firstname, surname,
                   date_of_birth, city, extra_field, modernized_only,
                   synced_at, created_at, updated_at
            FROM person
            ORDER BY synced_at DESC
        """)

        persons = cursor.fetchall()
        cursor.close()
        conn.close()

        # Convert to list of dicts
        result = [dict(p) for p in persons]

        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/persons', methods=['POST'])
def create_person():
    """Write-through: Write to legacy system (will sync back via CDC)"""
    try:
        data = request.json
        tenant_id = data.get('tenant_id')
        firstname = data.get('firstname')
        surname = data.get('surname')
        date_of_birth = data.get('date_of_birth')
        city = data.get('city')
        extra_field = data.get('extra_field')

        if not tenant_id or not firstname or not surname:
            emit('api', 'POST /api/persons rejected: tenant_id, firstname, and surname are required',
                 severity='warn', tenant_id=tenant_id)
            return jsonify({'error': 'tenant_id, firstname, and surname are required'}), 400

        config = LEGACY_DB_CONFIG.get(tenant_id)
        if not config:
            emit('api', f'POST /api/persons rejected: unknown tenant {tenant_id}', severity='warn')
            return jsonify({'error': f'Unknown tenant: {tenant_id}'}), 400

        # Write to legacy database
        conn = get_legacy_conn(tenant_id)
        cursor = conn.cursor()

        if config['type'] == 'legacy_a':
            # Legacy A schema: data_1, data_2, data_3
            # Modern system only knows about data_2 (mapped from extra_field)
            cursor.execute("""
                INSERT INTO person (firstname, surname, date_of_birth, city, data_1, data_2, data_3)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (firstname, surname, date_of_birth, city, None, extra_field or None, None))
        else:
            # Legacy B schema: data_a, data_b, data_c
            # Modern system only knows about data_c (mapped from extra_field)
            cursor.execute("""
                INSERT INTO person (firstname, surname, date_of_birth, city, data_a, data_b, data_c)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (firstname, surname, date_of_birth, city, None, None, extra_field or None))

        legacy_id = cursor.fetchone()['id']
        conn.commit()
        cursor.close()
        conn.close()

        emit('api', f'{tenant_id} person#{legacy_id} INSERT written to legacy', tenant_id=tenant_id,
             legacy_id=legacy_id)
        return jsonify({
            'success': True,
            'message': f'Written to {tenant_id} legacy system (ID: {legacy_id}). Will sync back via CDC.',
            'legacy_id': legacy_id,
            'tenant_id': tenant_id
        })

    except Exception as e:
        emit('api', f'POST /api/persons failed: {e}', severity='error', detail={'error': str(e)})
        return jsonify({'error': str(e)}), 500


@app.route('/api/persons', methods=['PUT'])
def update_person():
    """Update person via write-through to legacy system"""
    try:
        data = request.json
        tenant_id = data.get('tenant_id')
        legacy_id = data.get('legacy_id')

        if not tenant_id or not legacy_id:
            emit('api', 'PUT /api/persons rejected: tenant_id and legacy_id are required', severity='warn',
                 tenant_id=tenant_id)
            return jsonify({'error': 'tenant_id and legacy_id are required'}), 400

        config = LEGACY_DB_CONFIG.get(tenant_id)
        if not config:
            emit('api', f'PUT /api/persons rejected: unknown tenant {tenant_id}', severity='warn')
            return jsonify({'error': f'Unknown tenant: {tenant_id}'}), 400

        # Update in legacy database
        conn = get_legacy_conn(tenant_id)
        cursor = conn.cursor()

        if config['type'] == 'legacy_a':
            cursor.execute("""
                UPDATE person
                SET firstname = %s, surname = %s, date_of_birth = %s,
                    city = %s, data_2 = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                data.get('firstname'),
                data.get('surname'),
                data.get('date_of_birth'),
                data.get('city'),
                data.get('extra_field', ''),
                legacy_id
            ))
        else:
            cursor.execute("""
                UPDATE person
                SET firstname = %s, surname = %s, date_of_birth = %s,
                    city = %s, data_c = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                data.get('firstname'),
                data.get('surname'),
                data.get('date_of_birth'),
                data.get('city'),
                data.get('extra_field', ''),
                legacy_id
            ))

        conn.commit()
        cursor.close()
        conn.close()

        # Update modernized_only field in new system if provided
        if data.get('modernized_only'):
            new_conn = get_new_system_conn()
            new_cursor = new_conn.cursor()
            new_cursor.execute("""
                UPDATE person
                SET modernized_only = %s
                WHERE tenant_id = %s AND legacy_id = %s
            """, (data.get('modernized_only'), tenant_id, legacy_id))
            new_conn.commit()
            new_cursor.close()
            new_conn.close()

        emit('api', f'{tenant_id} person#{legacy_id} UPDATE written to legacy', tenant_id=tenant_id,
             legacy_id=legacy_id)
        return jsonify({
            'success': True,
            'message': 'Updated in legacy system. Will sync back via CDC.'
        })

    except Exception as e:
        emit('api', f'PUT /api/persons failed: {e}', severity='error', detail={'error': str(e)})
        return jsonify({'error': str(e)}), 500


@app.route('/api/legacy/<tenant_id>/persons', methods=['GET'])
def get_legacy_persons(tenant_id):
    """Direct read from legacy system (for demo/debugging)"""
    try:
        config = LEGACY_DB_CONFIG.get(tenant_id)
        if not config:
            return jsonify({'error': f'Unknown tenant: {tenant_id}'}), 400

        conn = get_legacy_conn(tenant_id)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM person ORDER BY id DESC")
        persons = cursor.fetchall()
        cursor.close()
        conn.close()

        result = [dict(p) for p in persons]
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/legacy/<tenant_id>/persons', methods=['POST'])
def create_legacy_person(tenant_id):
    """Direct write to legacy system (for demo/testing)"""
    try:
        data = request.json
        config = LEGACY_DB_CONFIG.get(tenant_id)
        if not config:
            return jsonify({'error': f'Unknown tenant: {tenant_id}'}), 400

        conn = get_legacy_conn(tenant_id)
        cursor = conn.cursor()

        if config['type'] == 'legacy_a':
            # Use provided values, or None (NULL) if empty
            data_1 = data.get('data_1') if data.get('data_1') else None
            data_2 = data.get('data_2') if data.get('data_2') else None
            data_3 = data.get('data_3') if data.get('data_3') else None

            cursor.execute("""
                INSERT INTO person (firstname, surname, date_of_birth, city, data_1, data_2, data_3)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data.get('firstname'),
                data.get('surname'),
                data.get('date_of_birth'),
                data.get('city'),
                data_1,
                data_2,
                data_3
            ))
        else:
            # Use provided values, or None (NULL) if empty
            data_a = data.get('data_a') if data.get('data_a') else None
            data_b = data.get('data_b') if data.get('data_b') else None
            data_c = data.get('data_c') if data.get('data_c') else None

            cursor.execute("""
                INSERT INTO person (firstname, surname, date_of_birth, city, data_a, data_b, data_c)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data.get('firstname'),
                data.get('surname'),
                data.get('date_of_birth'),
                data.get('city'),
                data_a,
                data_b,
                data_c
            ))

        legacy_id = cursor.fetchone()['id']
        conn.commit()
        cursor.close()
        conn.close()

        emit('api', f'{tenant_id} person#{legacy_id} INSERT written to legacy', tenant_id=tenant_id,
             legacy_id=legacy_id)
        return jsonify({
            'success': True,
            'legacy_id': legacy_id
        })

    except Exception as e:
        emit('api', f'POST /api/legacy/{tenant_id}/persons failed: {e}', severity='error', tenant_id=tenant_id,
             detail={'error': str(e)})
        return jsonify({'error': str(e)}), 500


@app.route('/api/legacy/<tenant_id>/persons/<int:record_id>', methods=['GET'])
def get_legacy_person(tenant_id, record_id):
    """Get single legacy person record"""
    try:
        config = LEGACY_DB_CONFIG.get(tenant_id)
        if not config:
            return jsonify({'error': f'Unknown tenant: {tenant_id}'}), 400

        conn = get_legacy_conn(tenant_id)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM person WHERE id = %s", (record_id,))
        person = cursor.fetchone()
        cursor.close()
        conn.close()

        if not person:
            return jsonify({'error': 'Record not found'}), 404

        return jsonify(dict(person))

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/legacy/<tenant_id>/persons/<int:record_id>', methods=['PUT'])
def update_legacy_person(tenant_id, record_id):
    """Direct update to legacy system"""
    try:
        data = request.json
        config = LEGACY_DB_CONFIG.get(tenant_id)
        if not config:
            return jsonify({'error': f'Unknown tenant: {tenant_id}'}), 400

        conn = get_legacy_conn(tenant_id)
        cursor = conn.cursor()

        # First, get the current record to preserve fields not being updated
        cursor.execute("SELECT * FROM person WHERE id = %s", (record_id,))
        current = cursor.fetchone()
        if not current:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Record not found'}), 404

        if config['type'] == 'legacy_a':
            # Only update data_1, data_2, data_3 if they have non-empty values
            # Otherwise preserve the existing values
            data_1 = data.get('data_1') if data.get('data_1') else current['data_1']
            data_2 = data.get('data_2') if data.get('data_2') else current['data_2']
            data_3 = data.get('data_3') if data.get('data_3') else current['data_3']

            cursor.execute("""
                UPDATE person
                SET firstname = %s, surname = %s, date_of_birth = %s,
                    city = %s, data_1 = %s, data_2 = %s, data_3 = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                data.get('firstname'),
                data.get('surname'),
                data.get('date_of_birth'),
                data.get('city'),
                data_1,
                data_2,
                data_3,
                record_id
            ))
        else:
            # Only update data_a, data_b, data_c if they have non-empty values
            # Otherwise preserve the existing values
            data_a = data.get('data_a') if data.get('data_a') else current['data_a']
            data_b = data.get('data_b') if data.get('data_b') else current['data_b']
            data_c = data.get('data_c') if data.get('data_c') else current['data_c']

            cursor.execute("""
                UPDATE person
                SET firstname = %s, surname = %s, date_of_birth = %s,
                    city = %s, data_a = %s, data_b = %s, data_c = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                data.get('firstname'),
                data.get('surname'),
                data.get('date_of_birth'),
                data.get('city'),
                data_a,
                data_b,
                data_c,
                record_id
            ))

        conn.commit()
        cursor.close()
        conn.close()

        emit('api', f'{tenant_id} person#{record_id} UPDATE written to legacy', tenant_id=tenant_id,
             legacy_id=record_id)
        return jsonify({'success': True})

    except Exception as e:
        emit('api', f'PUT /api/legacy/{tenant_id}/persons/{record_id} failed: {e}', severity='error',
             tenant_id=tenant_id, legacy_id=record_id, detail={'error': str(e)})
        return jsonify({'error': str(e)}), 500


def connector_states():
    """Debezium connector name -> state (RUNNING, FAILED, ...). Raises if Debezium cannot be reached."""
    with urllib.request.urlopen(f'{DEBEZIUM_URL}/connectors?expand=status', timeout=2) as resp:
        statuses = json.load(resp)
    return {name: info['status']['connector']['state'] for name, info in sorted(statuses.items())}


def monitor_connectors(states=connector_states, sleep=time.sleep, iterations=None):
    """Emit a `connector` event whenever a connector (or Debezium itself) changes state."""
    previous = {}
    count = 0
    while iterations is None or count < iterations:
        count += 1
        try:
            current = states()
        except Exception as e:
            current = {'debezium': 'UNREACHABLE'}
            detail = {'error': str(e)}
        else:
            detail = None
        for name, state in current.items():
            # a connector first seen RUNNING is not news
            old = previous.get(name, 'RUNNING' if state == 'RUNNING' else None)
            if state != old:
                severity = 'info' if state == 'RUNNING' else 'error' if state in ('FAILED', 'UNREACHABLE') else 'warn'
                emit('connector', f'{name}: {old or "unknown"} -> {state}', severity=severity,
                     detail=detail or {'connector': name, 'from': old, 'to': state})
        if 'debezium' not in current:
            previous.pop('debezium', None)
        previous.update(current)
        sleep(CONNECTOR_POLL_INTERVAL)


@app.route('/api/events', methods=['GET'])
def get_events():
    """Pipeline events, oldest first. `since_id` returns only newer events (for polling); without it, the newest."""
    try:
        since_id = request.args.get('since_id', default=0, type=int)
        limit = min(max(request.args.get('limit', default=200, type=int), 1), 1000)
        where, params = ['id > %s'], [since_id]
        severities = [v for v in request.args.get('severity', '').split(',') if v]
        if severities:
            where.append('severity = ANY(%s)')
            params.append(severities)
        if request.args.get('tenant'):
            where.append('tenant_id = %s')
            params.append(request.args['tenant'])
        # first page: the newest `limit` events; polling: everything after since_id
        order = 'ASC' if since_id else 'DESC'

        conn = get_new_system_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM pipeline_event WHERE {' AND '.join(where)} ORDER BY id {order} LIMIT %s",
                           params + [limit])
            rows = cursor.fetchall()
        except psycopg2.errors.UndefinedTable:
            rows = []  # nothing has emitted an event yet
        cursor.close()
        conn.close()

        if order == 'DESC':
            rows.reverse()
        for row in rows:
            row['ts'] = row['ts'].isoformat()
        return jsonify(rows)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/pipeline/status', methods=['GET'])
def pipeline_status():
    """Snapshot for the UI's status line. Each part degrades to null on its own."""
    status = {'connectors': None, 'staging_backlog': None, 'dead_letters': None,
              'last_consumer_activity': None, 'errors': None, 'events_last_minute': None}
    try:
        status['connectors'] = connector_states()
    except Exception as e:
        status['connectors'] = {'debezium': 'UNREACHABLE'}
        status['connectors_error'] = str(e)

    queries = {
        'staging_backlog': "SELECT count(*) AS v FROM person_staging WHERE processed = FALSE",
        'dead_letters': "SELECT count(*) AS v FROM person_dead_letter",
        'last_consumer_activity': "SELECT max(ts) AS v FROM pipeline_event WHERE source = 'cdc-consumer'",
        'errors': "SELECT count(*) AS v FROM pipeline_event WHERE severity = 'error'",
        'events_last_minute': "SELECT count(*) AS v FROM pipeline_event WHERE ts > now() - interval '1 minute'",
    }
    try:
        conn = get_new_system_conn()
        conn.autocommit = True  # one missing table must not fail the other queries
        cursor = conn.cursor()
        for key, sql in queries.items():
            try:
                cursor.execute(sql)
                value = cursor.fetchone()['v']
                status[key] = value.isoformat() if hasattr(value, 'isoformat') else value
            except psycopg2.Error:
                pass
        conn.close()
    except Exception as e:
        status['database_error'] = str(e)
    return jsonify(status)


if __name__ == '__main__':
    # Wait for databases to be ready
    print("Waiting for databases...")
    time.sleep(10)

    # debug=True re-runs this script in a reloader child; only that one serves requests
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        threading.Thread(target=monitor_connectors, daemon=True).start()

    print("Starting API server on port 5000...")
    app.run(host='0.0.0.0', port=5000, debug=True)

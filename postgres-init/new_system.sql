-- New System Multi-Tenant Database Schema

-- Tenant mapping table
CREATE TABLE tenant_mapping (
    tenant_id VARCHAR(50) PRIMARY KEY,
    tenant_name VARCHAR(100) NOT NULL,
    legacy_type VARCHAR(20) NOT NULL,  -- 'legacy_a' or 'legacy_b'
    db_host VARCHAR(100) NOT NULL,
    db_name VARCHAR(100) NOT NULL
);

-- Insert tenant mappings
INSERT INTO tenant_mapping (tenant_id, tenant_name, legacy_type, db_host, db_name) VALUES
    ('tenant_a1', 'Legacy A Instance 1', 'legacy_a', 'postgres-legacy-a1', 'legacy_a1'),
    ('tenant_a2', 'Legacy A Instance 2', 'legacy_a', 'postgres-legacy-a2', 'legacy_a2'),
    ('tenant_b1', 'Legacy B Instance 1', 'legacy_b', 'postgres-legacy-b1', 'legacy_b1'),
    ('tenant_b2', 'Legacy B Instance 2', 'legacy_b', 'postgres-legacy-b2', 'legacy_b2');

-- Staging table for raw CDC events
CREATE TABLE person_staging (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR(50) NOT NULL,
    source_id INTEGER NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    operation VARCHAR(10) NOT NULL,  -- 'INSERT', 'UPDATE', 'DELETE'
    firstname VARCHAR(100),
    surname VARCHAR(100),
    date_of_birth DATE,
    city VARCHAR(100),
    data_1 VARCHAR(100),
    data_2 VARCHAR(100),
    data_3 VARCHAR(100),
    data_a VARCHAR(100),
    data_b VARCHAR(100),
    data_c VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Final unified person table (dbt transforms staging -> here)
CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR(50) NOT NULL,
    legacy_id INTEGER NOT NULL,
    firstname VARCHAR(100) NOT NULL,
    surname VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    city VARCHAR(100),
    extra_field VARCHAR(100),  -- Mapped from data_2 (Legacy A) or data_c (Legacy B)
    modernized_only DATE,  -- Field that exists only in new system
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, legacy_id)
);

-- Indexes for performance
CREATE INDEX idx_person_tenant_id ON person(tenant_id);
CREATE INDEX idx_person_staging_processed ON person_staging(processed);
CREATE INDEX idx_person_staging_tenant ON person_staging(tenant_id);

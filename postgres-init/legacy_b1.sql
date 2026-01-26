-- Legacy B Tenant 1 Database Schema
-- Schema: data_a, data_b, data_c

CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    firstname VARCHAR(100) NOT NULL,
    surname VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    city VARCHAR(100),
    data_a VARCHAR(100),
    data_b VARCHAR(100),
    data_c VARCHAR(100),  -- Maps to extra_field in new system
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data
INSERT INTO person (firstname, surname, date_of_birth, city, data_a, data_b, data_c) VALUES
    ('Eve', 'Davis', '1991-09-12', 'Phoenix', 'alpha1', 'beta1', 'extra_b1_1'),
    ('Frank', 'Miller', '1987-11-30', 'Philadelphia', 'alpha2', 'beta2', 'extra_b1_2');

-- Create index for better performance
CREATE INDEX idx_person_updated_at ON person(updated_at);

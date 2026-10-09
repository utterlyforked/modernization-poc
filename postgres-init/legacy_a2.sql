-- Legacy A Tenant 2 Database Schema
-- Schema: data_1, data_2, data_3

CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    firstname VARCHAR(100) NOT NULL,
    surname VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    city VARCHAR(100),
    data_1 VARCHAR(100),
    data_2 VARCHAR(100),  -- Maps to extra_field in new system
    data_3 VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data
INSERT INTO person (firstname, surname, date_of_birth, city, data_1, data_2, data_3) VALUES
    ('Charlie', 'Williams', '1992-03-10', 'Chicago', 'val1', 'extra_a2_1', 'val3'),
    ('Diana', 'Brown', '1988-07-25', 'Houston', 'val4', 'extra_a2_2', 'val6');

-- Create index for better performance
CREATE INDEX idx_person_updated_at ON person(updated_at);

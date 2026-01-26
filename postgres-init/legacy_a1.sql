-- Legacy A Tenant 1 Database Schema
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
    ('Alice', 'Smith', '1990-01-15', 'New York', 'value1', 'extra_a1_1', 'value3'),
    ('Bob', 'Johnson', '1985-05-20', 'Los Angeles', 'value4', 'extra_a1_2', 'value6');

-- Create index for better performance
CREATE INDEX idx_person_updated_at ON person(updated_at);

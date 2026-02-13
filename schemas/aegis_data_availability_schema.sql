-- Aegis Data Availability Table Schema
-- Tracks which banks and periods have data available in each database

-- Create main data availability table
CREATE TABLE aegis_data_availability (
    -- Primary key
    id SERIAL PRIMARY KEY,

    -- Bank identification
    bank_id INTEGER NOT NULL,
    bank_name VARCHAR(100) NOT NULL,
    bank_symbol VARCHAR(10) NOT NULL,
    bank_aliases TEXT[],
    bank_tags TEXT[],

    -- Period identification
    fiscal_year INTEGER NOT NULL,
    quarter VARCHAR(2) NOT NULL CHECK (quarter IN ('Q1', 'Q2', 'Q3', 'Q4')),

    -- Database availability
    database_names TEXT[],

    -- Tracking
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by VARCHAR(100),

    -- Unique constraint to prevent duplicates
    UNIQUE(bank_id, fiscal_year, quarter)
);

-- Create indexes for performance
CREATE INDEX idx_aegis_bank ON aegis_data_availability(bank_id);
CREATE INDEX idx_aegis_period ON aegis_data_availability(fiscal_year, quarter);
CREATE INDEX idx_aegis_bank_period ON aegis_data_availability(bank_id, fiscal_year, quarter);
-- Aegis Reports Table Schema
-- Stores pre-generated reports with links to documents and markdown content

-- Create main reports table
CREATE TABLE aegis_reports (
    -- Primary key
    id SERIAL PRIMARY KEY,

    -- Report identification
    report_name VARCHAR(200) NOT NULL,
    report_description TEXT NOT NULL,
    report_type VARCHAR(100) NOT NULL,

    -- Bank and period
    bank_id INTEGER NOT NULL,
    bank_name VARCHAR(100) NOT NULL,
    bank_symbol VARCHAR(10) NOT NULL,
    fiscal_year INTEGER NOT NULL,
    quarter VARCHAR(2) NOT NULL CHECK (quarter IN ('Q1', 'Q2', 'Q3', 'Q4')),

    -- File locations
    local_filepath TEXT,
    s3_document_name TEXT,
    s3_pdf_name TEXT,

    -- Content
    markdown_content TEXT,

    -- Metadata
    generation_date TIMESTAMP NOT NULL,
    date_last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    generated_by VARCHAR(100),
    execution_id UUID,
    metadata JSONB,

    -- Unique constraint to prevent duplicates
    UNIQUE(bank_id, fiscal_year, quarter, report_type)
);

-- Create indexes for performance
CREATE INDEX idx_aegis_reports_bank ON aegis_reports(bank_id);
CREATE INDEX idx_aegis_reports_period ON aegis_reports(fiscal_year, quarter);
CREATE INDEX idx_aegis_reports_bank_period ON aegis_reports(bank_id, fiscal_year, quarter);
CREATE INDEX idx_aegis_reports_type ON aegis_reports(report_type);
CREATE INDEX idx_aegis_reports_generation_date ON aegis_reports(generation_date);
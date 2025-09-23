-- Aegis Tables Schema
-- Tables for aegis_data_availability and aegis_transcripts with embeddings

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

-- Create main transcripts table with embeddings
CREATE TABLE aegis_transcripts (
    -- Primary key
    id SERIAL PRIMARY KEY,

    -- File metadata
    file_path TEXT,
    filename TEXT,
    date_last_modified TIMESTAMP WITH TIME ZONE,

    -- Transcript identification
    title TEXT,
    transcript_type TEXT,
    event_id TEXT,
    version_id TEXT,

    -- Time and location identifiers
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter TEXT NOT NULL,
    institution_type TEXT,
    institution_id TEXT,
    ticker TEXT NOT NULL,
    company_name TEXT,

    -- Section and structure
    section_name TEXT,
    speaker_block_id INTEGER,
    qa_group_id INTEGER,

    -- Classifications
    classification_ids TEXT[],
    classification_names TEXT[],

    -- Summary
    block_summary TEXT,

    -- Chunk information
    chunk_id INTEGER,
    chunk_tokens INTEGER,
    chunk_content TEXT,
    chunk_paragraph_ids TEXT[],

    -- Embedding (3072 dimensions for text-embedding-3-large)
    chunk_embedding halfvec(3072),

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_aegis_transcripts_ticker ON aegis_transcripts(ticker);
CREATE INDEX idx_aegis_transcripts_fiscal_period ON aegis_transcripts(fiscal_year, fiscal_quarter);
CREATE INDEX idx_aegis_transcripts_ticker_period ON aegis_transcripts(ticker, fiscal_year, fiscal_quarter);
CREATE INDEX idx_aegis_transcripts_company_name ON aegis_transcripts(company_name);
CREATE INDEX idx_aegis_transcripts_event_id ON aegis_transcripts(event_id);

-- Create vector similarity search index
CREATE INDEX idx_aegis_transcripts_embedding ON aegis_transcripts
USING ivfflat (chunk_embedding vector_cosine_ops)
WITH (lists = 100);
-- Aegis Tables Schema
-- Generated: 2025-09-17
-- Tables: aegis_data_availability, aegis_transcripts

-- =====================================================
-- Table: aegis_data_availability
-- Purpose: Tracks which databases have data for each bank/quarter
-- =====================================================

CREATE TABLE aegis_data_availability (
    id SERIAL PRIMARY KEY,
    bank_id INTEGER NOT NULL,                          -- Unique identifier for the bank
    bank_name VARCHAR(100) NOT NULL,                   -- Full bank name
    bank_symbol VARCHAR(10) NOT NULL,                  -- Stock ticker symbol
    bank_aliases TEXT[],                               -- Alternative names for the bank
    bank_tags TEXT[],                                  -- Tags for categorization
    fiscal_year INTEGER NOT NULL,                      -- Fiscal year (e.g., 2024)
    quarter VARCHAR(2) NOT NULL,                       -- Fiscal quarter (Q1, Q2, Q3, Q4)
    database_names TEXT[],                             -- Available databases for this bank/period
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,  -- Record update timestamp
    last_updated_by VARCHAR(100),                      -- User who last updated the record
    CONSTRAINT aegis_data_availability_quarter_check CHECK (quarter IN ('Q1', 'Q2', 'Q3', 'Q4')),
    CONSTRAINT aegis_data_availability_bank_id_fiscal_year_quarter_key UNIQUE (bank_id, fiscal_year, quarter)
);

-- Indexes for aegis_data_availability
CREATE INDEX idx_aegis_bank ON aegis_data_availability (bank_id);
CREATE INDEX idx_aegis_bank_period ON aegis_data_availability (bank_id, fiscal_year, quarter);
CREATE INDEX idx_aegis_period ON aegis_data_availability (fiscal_year, quarter);

-- =====================================================
-- Table: aegis_transcripts
-- Purpose: Stores earnings transcript chunks with embeddings
-- Requires: pgvector extension for vector(3072) type
-- =====================================================

-- Enable pgvector extension if not already enabled
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE aegis_transcripts (
    id SERIAL PRIMARY KEY,

    -- File metadata
    file_path TEXT,                                    -- Full path to source file
    filename TEXT,                                     -- Name of source file
    date_last_modified TIMESTAMP WITH TIME ZONE,       -- File modification timestamp

    -- Transcript identification
    title TEXT,                                        -- Transcript title
    transcript_type TEXT,                              -- Type of transcript (e.g., earnings call)
    event_id TEXT,                                     -- Unique event identifier
    version_id TEXT,                                   -- Version identifier for updates

    -- Time and location identifiers
    fiscal_year INTEGER NOT NULL,                      -- Fiscal year of the transcript
    fiscal_quarter TEXT NOT NULL,                      -- Fiscal quarter (Q1, Q2, Q3, Q4)
    institution_type TEXT,                             -- Type of institution (e.g., bank)
    institution_id TEXT,                               -- Unique institution identifier
    ticker TEXT NOT NULL,                              -- Stock ticker symbol
    company_name TEXT,                                 -- Full company name

    -- Section and structure
    section_name TEXT,                                 -- Section within transcript (e.g., Q&A, Prepared Remarks)
    speaker_block_id INTEGER,                          -- Identifier for speaker blocks
    qa_group_id INTEGER,                               -- Question-answer group identifier

    -- Classifications
    classification_ids TEXT[],                         -- Array of classification IDs
    classification_names TEXT[],                       -- Array of classification names

    -- Summary
    block_summary TEXT,                                -- AI-generated summary of the block

    -- Chunk information
    chunk_id INTEGER,                                  -- Unique chunk identifier within transcript
    chunk_tokens INTEGER,                              -- Number of tokens in chunk
    chunk_content TEXT,                                -- Actual text content of chunk
    chunk_paragraph_ids TEXT[],                        -- Array of paragraph identifiers in chunk

    -- Embedding (3072 dimensions for text-embedding-3-large)
    chunk_embedding halfvec(3072),                     -- Vector embedding for similarity search (16-bit precision)

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP   -- Record update timestamp
);

-- Indexes for aegis_transcripts
CREATE INDEX idx_aegis_transcripts_ticker ON aegis_transcripts (ticker);
CREATE INDEX idx_aegis_transcripts_fiscal_period ON aegis_transcripts (fiscal_year, fiscal_quarter);
CREATE INDEX idx_aegis_transcripts_ticker_period ON aegis_transcripts (ticker, fiscal_year, fiscal_quarter);
CREATE INDEX idx_aegis_transcripts_company_name ON aegis_transcripts (company_name);
CREATE INDEX idx_aegis_transcripts_event_id ON aegis_transcripts (event_id);

-- Index for vector similarity search (using pgvector)
-- ivfflat index for approximate nearest neighbor search
CREATE INDEX idx_aegis_transcripts_embedding ON aegis_transcripts
USING ivfflat (chunk_embedding vector_cosine_ops)
WITH (lists = 100);
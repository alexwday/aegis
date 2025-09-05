-- Aegis Transcripts Table - PostgreSQL Schema
-- Stores earnings transcript chunks with embeddings for semantic search
-- Requires pgvector extension for vector similarity search

-- Enable pgvector extension (requires PostgreSQL with pgvector installed)
CREATE EXTENSION IF NOT EXISTS vector;

-- Drop table if exists (careful in production!)
DROP TABLE IF EXISTS aegis_transcripts CASCADE;

-- Create main transcripts table
CREATE TABLE aegis_transcripts (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- File metadata
    file_path TEXT NOT NULL,
    filename TEXT NOT NULL,
    date_last_modified TIMESTAMP WITH TIME ZONE,
    
    -- Transcript identification
    title TEXT,
    transcript_type TEXT,  -- "Corrected", "Final", "E1", etc.
    event_id TEXT,
    version_id TEXT,
    
    -- Time and location identifiers
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter TEXT NOT NULL,  -- "Q1", "Q2", "Q3", "Q4"
    institution_type TEXT,
    institution_id TEXT,
    ticker TEXT NOT NULL,
    company_name TEXT,
    
    -- Section and structure
    section_name TEXT NOT NULL,  -- "MANAGEMENT DISCUSSION SECTION" or "Q&A"
    speaker_block_id INTEGER,  -- Sequential ID for speaker blocks
    qa_group_id INTEGER,  -- Q&A conversation group ID (NULL for MD sections)
    
    -- Classifications from Stage 6
    classification_ids TEXT[],  -- Array of classification IDs
    classification_names TEXT[],  -- Array of classification names
    
    -- Summary from Stage 7
    block_summary TEXT,
    
    -- Chunk information
    chunk_id INTEGER NOT NULL,  -- Sequential chunk number within block/group
    chunk_tokens INTEGER,  -- Number of tokens in this chunk
    chunk_content TEXT NOT NULL,  -- Formatted text with headers that gets embedded
    chunk_paragraph_ids TEXT[],  -- Array of original paragraph IDs in this chunk
    
    -- Embedding (3072 dimensions for text-embedding-3-large, using halfvec for storage efficiency)
    chunk_embedding halfvec(3072) NOT NULL,
    
    -- Timestamps for data management
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for efficient querying

-- Primary retrieval pattern: bank + year + quarter
CREATE INDEX idx_transcripts_ticker_year_quarter 
    ON aegis_transcripts(ticker, fiscal_year, fiscal_quarter);

-- Section filtering
CREATE INDEX idx_transcripts_section_name 
    ON aegis_transcripts(section_name);

-- Speaker block ordering and gap detection
CREATE INDEX idx_transcripts_speaker_block 
    ON aegis_transcripts(ticker, fiscal_year, fiscal_quarter, speaker_block_id);

-- Q&A group retrieval
CREATE INDEX idx_transcripts_qa_group 
    ON aegis_transcripts(ticker, fiscal_year, fiscal_quarter, qa_group_id)
    WHERE qa_group_id IS NOT NULL;

-- Category filtering using GIN index for array search
CREATE INDEX idx_transcripts_classification_ids 
    ON aegis_transcripts USING GIN(classification_ids);

CREATE INDEX idx_transcripts_classification_names 
    ON aegis_transcripts USING GIN(classification_names);

-- Vector similarity search using HNSW (Hierarchical Navigable Small World)
-- This is much faster than the default IVFFlat for similarity search
CREATE INDEX idx_transcripts_vector_hnsw 
    ON aegis_transcripts 
    USING hnsw(chunk_embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- Alternative: IVFFlat index (use if HNSW not available)
-- CREATE INDEX idx_transcripts_vector_ivfflat 
--     ON aegis_transcripts 
--     USING ivfflat(chunk_embedding vector_cosine_ops)
--     WITH (lists = 100);

-- Full-text search on chunk content
CREATE INDEX idx_transcripts_content_search 
    ON aegis_transcripts 
    USING GIN(to_tsvector('english', chunk_content));

-- Composite index for chunk expansion (get all chunks from same block)
CREATE INDEX idx_transcripts_chunk_expansion 
    ON aegis_transcripts(ticker, fiscal_year, fiscal_quarter, speaker_block_id, chunk_id);

-- Event and version tracking
CREATE INDEX idx_transcripts_event_version 
    ON aegis_transcripts(event_id, version_id);

-- Date-based queries
CREATE INDEX idx_transcripts_date_modified 
    ON aegis_transcripts(date_last_modified DESC);

-- Create update trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_aegis_transcripts_updated_at 
    BEFORE UPDATE ON aegis_transcripts 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Comments for documentation
COMMENT ON TABLE aegis_transcripts IS 'Aegis earnings transcript chunks with embeddings for semantic search and RAG';
COMMENT ON COLUMN aegis_transcripts.chunk_embedding IS '3072-dimensional halfvec (16-bit precision) from text-embedding-3-large model';
COMMENT ON COLUMN aegis_transcripts.speaker_block_id IS 'Sequential ID within transcript for speaker blocks (used for gap detection)';
COMMENT ON COLUMN aegis_transcripts.qa_group_id IS 'Sequential ID for Q&A conversation groups (NULL for MD sections)';
COMMENT ON COLUMN aegis_transcripts.classification_ids IS 'Array of financial category IDs from classification';
COMMENT ON COLUMN aegis_transcripts.chunk_content IS 'Formatted text with headers ([Speaker Name] for MD, [Q&A Group X] for Q&A)';

-- Example queries for the three retrieval methods:

-- Method 1: Retrieve entire section
-- SELECT * FROM aegis_transcripts 
-- WHERE ticker = 'JPM' AND fiscal_year = 2024 AND fiscal_quarter = 'Q1'
--   AND section_name = 'MANAGEMENT DISCUSSION SECTION'
-- ORDER BY speaker_block_id, chunk_id;

-- Method 2: Category filtering
-- SELECT * FROM aegis_transcripts 
-- WHERE ticker = 'JPM' AND fiscal_year = 2024 AND fiscal_quarter = 'Q1'
--   AND classification_ids && ARRAY['category1', 'category2']
-- ORDER BY speaker_block_id, chunk_id;

-- Method 3: Similarity search with cosine distance (halfvec)
-- SELECT *, 1 - (chunk_embedding <=> '[query_embedding_vector]'::halfvec) as similarity
-- FROM aegis_transcripts
-- WHERE ticker = 'JPM' AND fiscal_year = 2024 AND fiscal_quarter = 'Q1'
-- ORDER BY chunk_embedding <=> '[query_embedding_vector]'::halfvec
-- LIMIT 50;

-- Gap detection query for MD section
-- WITH blocks AS (
--   SELECT DISTINCT speaker_block_id 
--   FROM aegis_transcripts 
--   WHERE ticker = 'JPM' AND fiscal_year = 2024 AND fiscal_quarter = 'Q1'
--     AND section_name = 'MANAGEMENT DISCUSSION SECTION'
-- )
-- SELECT speaker_block_id + 1 as missing_block_id
-- FROM blocks b1
-- WHERE NOT EXISTS (
--   SELECT 1 FROM blocks b2 
--   WHERE b2.speaker_block_id = b1.speaker_block_id + 1
-- )
-- AND speaker_block_id < (SELECT MAX(speaker_block_id) FROM blocks);
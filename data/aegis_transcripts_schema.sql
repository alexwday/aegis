-- Aegis Transcripts Table Schema
-- Table structure for storing earnings transcript chunks with embeddings

-- Create main transcripts table
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
    chunk_embedding vector(3072),
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE EXTENSION IF NOT EXISTS vector;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS public."aegis-financial-supp-data" (
    source_type text NOT NULL,
    fiscal_year text NOT NULL,
    quarter text NOT NULL,
    bank text NOT NULL,
    filename text NOT NULL,
    file_id text NOT NULL,
    file_type text NOT NULL,
    file_path text NOT NULL,
    file_hash text NOT NULL,
    page_number integer,
    name text,
    summary text,
    chunk_id text NOT NULL,
    chunk_content text,
    keywords jsonb NOT NULL DEFAULT '[]'::jsonb,
    metrics jsonb NOT NULL DEFAULT '[]'::jsonb,
    keyword_embedding vector(3072),
    metric_embedding vector(3072),
    summary_embedding vector(3072),
    chunk_embedding vector(3072),
    created_at timestamptz,
    PRIMARY KEY (file_id, chunk_id)
);

CREATE TABLE IF NOT EXISTS public."aegis-financial-supp-embeddings" (
    embedding_id text NOT NULL,
    embedding_type text NOT NULL,
    embedding_scope text NOT NULL,
    source_type text NOT NULL,
    fiscal_year text NOT NULL,
    quarter text NOT NULL,
    bank text NOT NULL,
    filename text NOT NULL,
    file_id text NOT NULL,
    file_type text NOT NULL,
    file_path text NOT NULL,
    file_hash text NOT NULL,
    content_unit_id text,
    content_unit_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
    chunk_id text,
    section_id text,
    embedding_text text NOT NULL,
    text_hash text,
    embedding vector(3072),
    embedding_model text NOT NULL,
    embedding_dimensions integer NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (embedding_id)
);

CREATE INDEX IF NOT EXISTS idx_fin_supp_data_bank_period
ON public."aegis-financial-supp-data" (bank, fiscal_year, quarter);

CREATE INDEX IF NOT EXISTS idx_fin_supp_data_file_chunk_pattern
ON public."aegis-financial-supp-data" (file_id, chunk_id text_pattern_ops);

CREATE INDEX IF NOT EXISTS idx_fin_supp_data_keywords_trgm
ON public."aegis-financial-supp-data"
USING gin ((COALESCE(keywords::text, '')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_fin_supp_data_metrics_trgm
ON public."aegis-financial-supp-data"
USING gin ((COALESCE(metrics::text, '')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_fin_supp_data_fts
ON public."aegis-financial-supp-data"
USING gin (
    (
        setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(summary, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(keywords::text, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(metrics::text, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(chunk_content, '')), 'C')
    )
);

CREATE INDEX IF NOT EXISTS idx_fin_supp_embeddings_type_bank_period
ON public."aegis-financial-supp-embeddings" (embedding_type, bank, fiscal_year, quarter);

CREATE INDEX IF NOT EXISTS idx_fin_supp_embeddings_type_file_chunk
ON public."aegis-financial-supp-embeddings" (
    embedding_type,
    file_id,
    (COALESCE(chunk_id, content_unit_id))
);

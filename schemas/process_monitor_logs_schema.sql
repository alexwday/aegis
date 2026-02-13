-- Process Monitor Logs Table Schema
-- Tracks execution and performance metrics for all workflow stages

-- Create main process monitor logs table
CREATE TABLE process_monitor_logs (
    -- Primary key
    log_id BIGSERIAL PRIMARY KEY,

    -- Run identification
    run_uuid UUID NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    stage_name VARCHAR(100) NOT NULL,

    -- Timing
    stage_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    stage_end_time TIMESTAMP WITH TIME ZONE,
    duration_ms INTEGER,

    -- LLM usage tracking
    llm_calls JSONB,
    total_tokens INTEGER,
    total_cost NUMERIC(12,6),

    -- Execution details
    status VARCHAR(255),
    decision_details TEXT,
    error_message TEXT,

    -- Metadata
    log_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    user_id VARCHAR(255),
    environment VARCHAR(50),
    custom_metadata JSONB,
    notes TEXT
);

-- Create indexes for performance
CREATE INDEX idx_process_monitor_logs_run_uuid ON process_monitor_logs(run_uuid);
CREATE INDEX idx_process_monitor_logs_model_name ON process_monitor_logs(model_name);
CREATE INDEX idx_process_monitor_logs_stage_name ON process_monitor_logs(stage_name);
CREATE INDEX idx_process_monitor_logs_model_stage ON process_monitor_logs(model_name, stage_name);
CREATE INDEX idx_process_monitor_logs_stage_start_time ON process_monitor_logs(stage_start_time);
CREATE INDEX idx_process_monitor_logs_status ON process_monitor_logs(status);
CREATE INDEX idx_process_monitor_logs_environment ON process_monitor_logs(environment);
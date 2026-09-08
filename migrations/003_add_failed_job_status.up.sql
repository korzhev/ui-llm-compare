ALTER TYPE job_status
ADD VALUE IF NOT EXISTS 'failed' AFTER 'done';

BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM jobs WHERE status = 'failed') THEN
        RAISE EXCEPTION 'cannot remove job_status value "failed": jobs with this status exist';
    END IF;
END
$$;

ALTER TABLE jobs
ALTER COLUMN status DROP DEFAULT;

ALTER TYPE job_status RENAME TO job_status_old;

CREATE TYPE job_status AS ENUM (
    'created',
    'pending',
    'done'
);

ALTER TABLE jobs
ALTER COLUMN status TYPE job_status
USING status::TEXT::job_status;

ALTER TABLE jobs
ALTER COLUMN status SET DEFAULT 'created';

DROP TYPE job_status_old;

COMMIT;
CREATE TABLE IF NOT EXISTS public.scanned_ip_ranges (
    addr_range VARCHAR(15) PRIMARY KEY NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
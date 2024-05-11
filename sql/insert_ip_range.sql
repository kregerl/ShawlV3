INSERT INTO public.scanned_ip_ranges (addr_range, created_at)
VALUES ($1, $2) ON CONFLICT (addr_range) DO NOTHING;
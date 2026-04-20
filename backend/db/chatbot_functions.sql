-- 1. Function to get a spending summary for the AI context
CREATE OR REPLACE FUNCTION get_user_spending_summary(p_user_id UUID, p_months INT DEFAULT 3)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    result JSONB;
BEGIN
    WITH monthly_stats AS (
        SELECT 
            TO_CHAR(transaction_date, 'YYYY-MM') AS month,
            transaction_type,
            SUM(amount) as total_amount,
            COUNT(*) as txn_count
        FROM transactions
        WHERE user_id = p_user_id
          AND transaction_date >= CURRENT_DATE - (p_months || ' months')::INTERVAL
        GROUP BY 1, 2
    ),
    category_stats AS (
        SELECT 
            a.account_name as category,
            SUM(t.amount) as total_amount
        FROM transactions t
        JOIN accounts a ON t.offset_account_id = a.account_id
        WHERE t.user_id = p_user_id
          AND t.transaction_type = 'DEBIT'
          AND t.transaction_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 5
    )
    SELECT jsonb_build_object(
        'monthly_history', (SELECT jsonb_agg(monthly_stats) FROM monthly_stats),
        'top_categories_30d', (SELECT jsonb_agg(category_stats) FROM category_stats)
    ) INTO result;
    
    RETURN result;
END;
$$;

-- 2. Function to detect anomalies/leaks for the AI
CREATE OR REPLACE FUNCTION get_spending_anomalies(p_user_id UUID)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    result JSONB;
BEGIN
    WITH category_averages AS (
        -- Calculate avg spending per category over last 90 days (excluding last 7 days)
        SELECT 
            offset_account_id,
            SUM(amount) / 3 AS monthly_avg
        FROM transactions
        WHERE user_id = p_user_id
          AND transaction_type = 'DEBIT'
          AND transaction_date >= CURRENT_DATE - INTERVAL '97 days'
          AND transaction_date < CURRENT_DATE - INTERVAL '7 days'
        GROUP BY 1
    ),
    recent_spending AS (
        -- Get spending in last 7 days
        SELECT 
            offset_account_id,
            SUM(amount) AS recent_total
        FROM transactions
        WHERE user_id = p_user_id
          AND transaction_type = 'DEBIT'
          AND transaction_date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY 1
    ),
    leaks AS (
        SELECT 
            a.account_name,
            ca.monthly_avg,
            rs.recent_total,
            (rs.recent_total / NULLIF(ca.monthly_avg / 4.0, 0)) * 100 as increase_percent
        FROM recent_spending rs
        JOIN category_averages ca ON rs.offset_account_id = ca.offset_account_id
        JOIN accounts a ON rs.offset_account_id = a.account_id
        WHERE rs.recent_total > (ca.monthly_avg / 4.0) * 1.5 -- More than 50% above weekly avg
    )
    SELECT jsonb_agg(leaks) INTO result FROM leaks;
    
    RETURN COALESCE(result, '[]'::jsonb);
END;
$$;

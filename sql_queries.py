formatting_filtering_query = """
    SELECT 
        ticker AS stock,
        ROUND(open, 4) AS open,
        ROUND(close), 4) AS close,
        ROUND(adj_close), 4) AS adj_close,
        ROUND(low), 4) AS low,
        ROUND(high), 4) AS high,
        TO_DATE(date, 'yyyy-MM-dd') AS date
    FROM temp_hist_stock
    WHERE ticker IN ('AAPL', 'AMZN')
    """

moving_average_query = """
WITH ranked_data AS (
    SELECT 
        stock,
        date,
        open,
        close,
        adj_close,
        low,
        high,
        ROW_NUMBER() OVER (PARTITION BY stock ORDER BY date) as rn
    FROM temp_moving_average
)
SELECT 
    stock,
    date,
    open,
    close,
    adj_close,
    low,
    high,
    CASE WHEN rn >= 7 
        THEN ROUND(AVG(open) OVER (
            PARTITION BY stock 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4)
        ELSE NULL 
    END AS open_moving_avg_7_day,
    CASE WHEN rn >= 7 
        THEN ROUND(AVG(close) OVER (
            PARTITION BY stock 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4)
        ELSE NULL 
    END AS close_moving_avg_7_day,
    CASE WHEN rn >= 7 
        THEN ROUND(AVG(adj_close) OVER (
            PARTITION BY stock 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4)
        ELSE NULL 
    END AS adj_close_moving_avg_7_day,
    CASE WHEN rn >= 7 
        THEN ROUND(AVG(low) OVER (
            PARTITION BY stock 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4)
        ELSE NULL 
    END AS low_moving_avg_7_day,
    CASE WHEN rn >= 7 
        THEN ROUND(AVG(high) OVER (
            PARTITION BY stock 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4)
        ELSE NULL 
    END AS high_moving_avg_7_day
FROM ranked_data;
"""

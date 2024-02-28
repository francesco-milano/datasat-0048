with cte AS
(
    SELECT 
        convert(date, t.start_time) as run_date,
        sum(t.data_processed_mb) / 1024. as data_processed_GB,
        sum(t.total_elapsed_time_ms) / 1000 / 60 as total_elapsed_time_min
    FROM 
        sys.dm_exec_requests_history as t
    group BY
        convert(date, t.start_time)
)
SELECT
    format(cte.run_date, 'yyyy-MM-dd') as run_date,
    format(cte.data_processed_GB, 'N2') as data_processed_GB, 
    cte.total_elapsed_time_min,
    format(sum(cte.data_processed_GB) over (partition by year(cte.run_date) * 100 + month(cte.run_date)), 'N2')     
        as total_data_processed_monthly_GB
FROM
    cte
order by
    cte.run_date desc
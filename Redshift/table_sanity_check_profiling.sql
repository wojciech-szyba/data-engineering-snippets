SELECT 'SELECT ''' || column_name || ''', COUNT(1), MAX(' || column_name || ')' || ''', MIN(' || column_name || ')', || ''', SUM(CASE WHEN ' || column_name || ' IS NULL THEN 0 ELSE 1 END) UNION ALL'
FROM svv_columns 
WHERE table_schema = '...' 
  AND table_name = '...';

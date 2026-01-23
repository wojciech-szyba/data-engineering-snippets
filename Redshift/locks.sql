SELECT *,datediff(s,txn_start,getdate())/86400||' days '||datediff(s,txn_start,getdate())%86400/3600||' hrs '||datediff(s,txn_start,getdate())%3600/60||' mins '||datediff(s,txn_start,getdate())%60||' secs' AS "duration"
FROM svv_transactions 
WHERE lockable_object_type='transactionid' 
  AND pid<>pg_backend_pid() 
ORDER BY 3;

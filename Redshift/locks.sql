select *,datediff(s,txn_start,getdate())/86400||' days '||datediff(s,txn_start,getdate())%86400/3600||' hrs '||datediff(s,txn_start,getdate())%3600/60||' mins '||datediff(s,txn_start,getdate())%60||' secs' as "duration"
from svv_transactions 
where lockable_object_type='transactionid' 
  and pid<>pg_backend_pid() 
order by 3;

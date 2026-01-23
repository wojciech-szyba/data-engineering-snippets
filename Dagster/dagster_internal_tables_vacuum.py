from dagster import op, graph, OpExecutionContext
import psycopg2

DAGSTER_POSTGRES_DB = 'db_name'
DAGSTER_POSTGRES_USER = 'username'
DAGSTER_POSTGRES_PASSWORD = 'password'
DAGSTER_POSTGRES_HOST = 'host'


class DagsterInternalDB(object):
    def __init__(self, context=None):
        # Database connection parameters
        self.conn = psycopg2.connect(
                                     dbname=DAGSTER_POSTGRES_DB,
                                     user=DAGSTER_POSTGRES_USER,
                                     password=DAGSTER_POSTGRES_PASSWORD,
                                     host=DAGSTER_POSTGRES_HOST
        )

        self.cursor = self.conn.cursor()
        self.context = context

    def _do_query(self, query):
        self.cursor.execute(query)
        self.conn.commit()

    def vacuum(self):
        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(0)
        query = "VACUUM FULL event_logs;"
        self._do_query(query)
        query = "VACUUM FULL runs;"
        self._do_query(query)
        query = "VACUUM FULL run_tags;"
        self._do_query(query)
        self.conn.set_isolation_level(old_isolation_level)

        # Commit the transaction
        self.conn.commit()

@op
def vacuum_logs(context: OpExecutionContext):
    dagster_postgres = DagsterInternalDB(context)
    try:
        dagster_postgres.vacuum()        
    except Exception as e:
        context.log.warning(str(e))
    finally:
        dagster_postgres.close_connection()
        
        
@graph()
def vacuum_logs_graph():
    vacuum_logs()
    
    
vacuum_logs_graph_job = vacuum_logs_graph.to_job(
    name="vacuum_logs_graph_job"    
)

from dagster import op, graph, OpExecutionContext
from datetime import datetime, timedelta
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
        self.days = 30

    def delete_old_event_logs(self):
        # Calculate the timestamp for one month ago
        one_month_ago = datetime.now() - timedelta(days=self.days)

        # SQL query to delete logs older than one month
        delete_query = """
            DELETE FROM event_logs
            WHERE timestamp < %s                    
              AND event::jsonb->>'level' in ('10', '20', '30', '40') 
              AND dagster_event_type is null;
            """

        # Execute the query
        self.cursor.execute(delete_query, (one_month_ago,))

        # Commit the transaction
        self.conn.commit()

        delete_query = """
            DELETE FROM event_logs
            WHERE timestamp < %s    
              AND event::jsonb->>'level' in ('10', '20', '30', '40')
              AND dagster_event_type IN ('STEP_FAILURE', 'STEP_INPUT', 'LOADED_INPUT', 'STEP_UP_FOR_RETRY');
        """

        # Execute the query
        self.cursor.execute(delete_query, (one_month_ago,))

        # Commit the transaction
        self.conn.commit()


@op
def delete_old_logs(context: OpExecutionContext):
    dagster_postgres = DagsterInternalDB(context)
    try:
        dagster_postgres.delete_old_event_logs()        
    except Exception as e:
        context.log.warning(str(e))
    finally:
        dagster_postgres.close_connection()
        
        
@graph()
def delete_old_logs_graph():
    delete_old_logs()
    
    
delete_old_logs_graph_job = delete_old_logs_graph.to_job(
    name="delete_old_logs_graph"    
)
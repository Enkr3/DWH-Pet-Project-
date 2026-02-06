from airflow import DAG
from datetime import datetime, timedelta, date 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup 
from airflow.models import Variable


DB_CONN = "gp_std15_35"
DB_SCHEMA = 'std15_35'
DB_PROC_LOAD = 'f_load_full'
DB_DELTA_LOAD = 'f_load_delta_partitions'
FULL_LOAD_TABLES = ['stores', 'coupons', 'promo', 'promo_types']
FULL_LOAD_FILES = {'stores':'stores', 'coupons':'coupons', 'promo':'promo','promo_types':'promo_types'}
DELTA_LOAD_TABLES = ['traffic', 'bills_head', 'bills_item']
DELTA_PARTITION_KEY = {'traffic':'date', 'bills_head':'calday', 'bills_item':'calday'}
START_DATE = "'2021-01-01'"
END_DATE = "'2021-02-28'"
USER_AUT = "'intern'"
PASS_AUT = "'intern'"
MD_TABLE_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_PROC_LOAD}(%(tab_name)s, %(file_name)s);"

LOAD_PART_TABLE = f"select {DB_SCHEMA}.{DB_DELTA_LOAD}(%(file_name)s, %(tab_name)s, %(partition_key)s, {START_DATE}, {END_DATE}, {USER_AUT}, {PASS_AUT})"

DM_FUNC = f"select std15_35.f_report_dm({START_DATE}, {END_DATE})"

default_args = {
    'depends_on_past': False,
    'owner': 'enkr',
    'start_date':datetime(2026, 1, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "enkr_dag_finalproject",
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    
    task_start = DummyOperator(task_id="start")
    
    task_rep_dm = PostgresOperator(
        task_id="rep_dm",
        postgres_conn_id="gp_std15_35",
        sql=DM_FUNC
    )
    with TaskGroup("delta_insert") as task_delta_insert_tables:
        for table in DELTA_LOAD_TABLES:
            task = PostgresOperator(task_id = f"delta_load_table_{table}",
                                    postgres_conn_id = DB_CONN,
                                    sql = LOAD_PART_TABLE,
                                    parameters = {'file_name':f'gp.{table}', 'tab_name':f'std15_35.{table}', 'partition_key':f'{DELTA_PARTITION_KEY[table]}'}
                                   )
    
    
    with TaskGroup("full_insert") as task_full_insert_tables:
        for table in FULL_LOAD_TABLES:
            task = PostgresOperator(task_id = f"load_table_{table}",
                                    postgres_conn_id = DB_CONN,
                                    sql = MD_TABLE_LOAD_QUERY,
                                    parameters = {'tab_name':f'{DB_SCHEMA}.{table}', 'file_name':f'{FULL_LOAD_FILES[table]}'}
                                   )
            
    task_end = DummyOperator(task_id = 'end')
    
    task_start >> task_full_insert_tables >> task_delta_insert_tables >> task_rep_dm >> task_end

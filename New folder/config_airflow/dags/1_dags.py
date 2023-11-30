from datetime import datetime
import logging

from airflow import DAG
from airflow.models import Variable, Connection
from airflow.operators.python import PythonOperator

from modules.covid_scraper import CovidScraper
from modules.transformer import transformer
from modules.connector import Connector

def fun_get_data_from_api(**kwargs):
    # get data
    scraper = CovidScraper(Variable.get('url_covid_tracker'))
    data = scraper.get_data()
    print(data.info())


    get_conn = Connection.get_connection_from_secrets("Mysql")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host=get_conn.host,
        user=get_conn.login,
        password=get_conn.password,
        db=get_conn.schema,
        port=get_conn.port
    )

    # drop table if exists
    try:
        p = "DROP table IF EXISTS covid_jabar"
        engine_sql.execute(p)

    except Exception as e:
        logging.error(e)

    # insert to mysql
    data.to_sql(con=engine_sql, name="covid_jabar", index=False)
    logging.info("DATA INSERTED SUCCESSFULLY TO MYSQL")


def fun_generate_dim(**kwargs):
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host=get_conn_mysql.host,
        user=get_conn_mysql.login,
        password=get_conn_mysql.password,
        db=get_conn_mysql.schema,
        port=get_conn_mysql.port
    )

    engine_postgres = connector.connect_postgres(
        host=get_conn_postgres.host,
        user=get_conn_postgres.login,
        password=get_conn_postgres.password,
        db=get_conn_postgres.schema,
        port=get_conn_postgres.port
    )

    transformer = transformer(engine_sql, engine_postgres)
    transformer.transform_dim_case()
    transformer.transform_dim_district()
    transformer.transform_dim_province()

def fun_insert_province_daily(**kwargs):
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgre")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host=get_conn_mysql.host,
        user=get_conn_mysql.login,
        password=get_conn_mysql.password,
        db=get_conn_mysql.schema,
        port=get_conn_mysql.port
    )

    engine_postgres = connector.connect_postgres(
        host=get_conn_postgres.host,
        user=get_conn_postgres.login,
        password=get_conn_postgres.password,
        db=get_conn_postgres.schema,
        port=get_conn_postgres.port
    )

def fun_insert_district_daily(**kwargs):
    get_conn_mysql = Connection.get_connection_from_secrets("Mysql")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgre")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host=get_conn_mysql.host,
        user=get_conn_mysql.login,
        password=get_conn_mysql.password,
        db=get_conn_mysql.schema,
        port=get_conn_mysql.port
    )

    engine_postgres = connector.connect_postgres(
        host=get_conn_postgres.host,
        user=get_conn_postgres.login,
        password=get_conn_postgres.password,
        db=get_conn_postgres.schema,
        port=get_conn_postgres.port
    )

with DAG(
    dag_id='1_dags',
    start_date=datetime(2022, 5 , 28),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    op_get_data_from_api = PythonOperator(
        task_id='get_data_from_api',
        python_callable=fun_get_data_from_api
    )

op_get_data_from_api
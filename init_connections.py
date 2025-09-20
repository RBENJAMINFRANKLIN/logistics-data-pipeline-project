# init_connections.py
from airflow.models import Connection
from airflow import settings
import yaml

def load_snowflake_config():
    with open('dags/utils/snowflake_config.yaml') as f:
        config = yaml.safe_load(f)
    return config['snowflake']

def create_snowflake_connection(conn_id, config):
    session = settings.Session()
    try:
        existing_conn = session.query(Connection).filter_by(conn_id=conn_id).first()
        if existing_conn:
            session.delete(existing_conn)
            session.commit()

        conn = Connection(
            conn_id=conn_id,
            conn_type='snowflake',
            login=config['user'],
            password=config['password'],
            schema=config['schema'],
            extra={
                "account": config['account'],
                "warehouse": config['warehouse'],
                "database": config['database'],
                "role": config['role']
            }
        )
        session.add(conn)
        session.commit()
        print(f"✅ Snowflake connection '{conn_id}' created.")
    except Exception as e:
        session.rollback()
        print(f"❌ Failed to create connection: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    config = load_snowflake_config()
    create_snowflake_connection('snowflakeid', config)

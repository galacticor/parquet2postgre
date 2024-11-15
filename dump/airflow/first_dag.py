from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
from typing import List, Dict
from ssh_pipeline import (  # Assuming the original code is in ssh_pipeline.py
    SSHPipeline, 
    ServerConfig, 
    load_config
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def validate_config(**context):
    """Validate the SSH pipeline configuration"""
    try:
        config_path = Variable.get("ssh_pipeline_config_path", default_var="/opt/airflow/configs/ssh_config.json")
        servers = load_config(config_path)
        logger.info(f"Successfully validated configuration with {len(servers)} servers")
        
        # Pass the validated servers to the next task
        context['task_instance'].xcom_push(key='servers', value=[
            {
                'hostname': s.hostname,
                'username': s.username,
                'password': s.password,
                'key_path': s.key_path,
                'port': s.port,
                'weight': s.weight,
                'server_num': s.server_num
            } for s in servers
        ])
        return True
    except Exception as e:
        logger.error(f"Configuration validation failed: {str(e)}")
        raise

def run_ssh_pipeline(**context):
    """Execute the SSH pipeline with the validated configuration"""
    try:
        # Get servers from previous task
        servers_data = context['task_instance'].xcom_pull(task_ids='validate_config', key='servers')
        
        # Reconstruct ServerConfig objects
        servers = [ServerConfig(**server_data) for server_data in servers_data]
        
        # Get pipeline parameters from Airflow Variables
        pipeline_params = {
            'max_retries': int(Variable.get("ssh_pipeline_max_retries", default_var="3")),
            'pool_size': int(Variable.get("ssh_pipeline_pool_size", default_var="2")),
            'server_wait_timeout': float(Variable.get("ssh_pipeline_server_timeout", default_var="30")),
            'log_wait_timeout': float(Variable.get("ssh_pipeline_log_timeout", default_var="15")),
        }
        
        # Get batch command parameters
        base_command = Variable.get("ssh_pipeline_base_command", default_var="python3 test.py")
        batch_start = int(Variable.get("ssh_pipeline_batch_start", default_var="0"))
        batch_end = int(Variable.get("ssh_pipeline_batch_end", default_var="6"))
        batch_step = int(Variable.get("ssh_pipeline_batch_step", default_var="2"))
        num_workers = int(Variable.get("ssh_pipeline_num_workers", default_var="2"))
        
        # Initialize and run pipeline
        pipeline = SSHPipeline(servers, **pipeline_params)
        
        pipeline.process_batch_commands(
            base_command=base_command,
            batch_range=range(batch_start, batch_end, batch_step),
            num_workers=num_workers
        )
        
        logger.info("SSH pipeline execution completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise

# Create the DAG
dag = DAG(
    'ssh_distributed_pipeline',
    default_args=default_args,
    description='DAG for running distributed SSH pipeline jobs',
    schedule_interval=None,  # Set to None for manual triggers only
    start_date=days_ago(1),
    tags=['ssh', 'distributed'],
    catchup=False
)

# Define tasks
validate_task = PythonOperator(
    task_id='validate_config',
    python_callable=validate_config,
    provide_context=True,
    dag=dag,
)

run_pipeline_task = PythonOperator(
    task_id='run_ssh_pipeline',
    python_callable=run_ssh_pipeline,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
validate_task >> run_pipeline_task
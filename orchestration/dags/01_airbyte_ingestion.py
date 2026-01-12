from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests
import time
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
DBT_PROJECT_DIR = '/usr/local/airflow/include/analytics'
AIRFLOW_CONN_ID = 'airbyte_local_custom'

AIRBYTE_JOB_IDS = {
    'customers': 'd4aece53-aaba-4f96-81d4-ae7e1f3d01e5',
    'geolocation': 'e6089394-4964-40d0-8f53-1439fef2e6dd',
    'order_items': '887e440c-d220-4858-9069-a5e1556f62cf',
    'order_payments': 'a2be9652-d64e-4947-8439-47534c8d5bb2',
    'reviews': '1fe87d00-1d30-4d0b-a7f0-7cfb5edacb37',
    'orders': '7f4a56d0-1463-452b-85b0-cc858a24e46e',
    'product_translations': '77bacc88-4a95-46e3-ae12-604fb336ddc9',
    'products': '79da0a5d-516f-4a4a-8390-8d3ed255945a',
    'sellers': 'a69dc628-2ec7-48d7-8ec0-43c12cbed83a'
}

def get_airbyte_creds():
    """Retrieves Host, Client ID and Secret from Airflow Connection"""
    conn = BaseHook.get_connection(AIRFLOW_CONN_ID)
    return {
        "host": conn.host.rstrip('/'),
        "client_id": conn.login,
        "client_secret": conn.password
    }

def get_access_token(host, client_id, client_secret):
    """Authenticates using Client ID/Secret to get a Bearer Token"""
    token_url = f"{host}/api/v1/applications/token"
    
    # CRITICAL FIX: keys must be snake_case for some Airbyte versions
    payload = {
        "client_id": client_id,
        "client_secret": client_secret
    }
    
    try:
        print(f"Requesting token from {token_url}...")
        response = requests.post(token_url, json=payload, verify=False, timeout=10)
        
        # If snake_case fails, try camelCase as fallback (handling version differences)
        if response.status_code >= 400:
            print("snake_case failed, trying camelCase payload...")
            payload_fallback = {"clientId": client_id, "clientSecret": client_secret}
            response = requests.post(token_url, json=payload_fallback, verify=False, timeout=10)

        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        print(f"Token Error Response: {getattr(response, 'text', 'N/A')}")
        raise Exception(f"Auth failed: {str(e)}")

def trigger_airbyte_sync(connection_id, **kwargs):
    print(f"Starting sync for connection: {connection_id}")
    
    creds = get_airbyte_creds()
    host = creds['host']
    
    # 1. Get Token
    access_token = get_access_token(host, creds['client_id'], creds['client_secret'])
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # 2. Trigger Sync
    trigger_url = f"{host}/api/v1/connections/sync"
    
    try:
        response = requests.post(trigger_url, json={"connectionId": connection_id}, headers=headers, verify=False)
        
        if response.status_code == 409:
            print("Sync already running (Conflict).")
            return "Sync already running"
            
        response.raise_for_status()
        job_id = response.json().get("job", {}).get("id")
        print(f"Job {job_id} triggered successfully.")

    except Exception as e:
        raise Exception(f"Error triggering Airbyte: {str(e)}")

    # 3. Monitor Status
    job_status = "running"
    while job_status in ["running", "pending", "incomplete"]:
        time.sleep(10)
        
        check_url = f"{host}/api/v1/jobs/get"
        status_resp = requests.post(check_url, json={"id": job_id}, headers=headers, verify=False)
        status_resp.raise_for_status()
        
        job_info = status_resp.json().get("job", {})
        job_status = job_info.get("status")
        print(f"Job {job_id} status: {job_status}")
    
    if job_status != "succeeded":
        raise Exception(f"Job failed. Final status: {job_status}")
    
    print("Sync finished successfully!")

with DAG('01_airbyte_ingestion',
         start_date=datetime(2023, 1, 1),
         schedule='@daily',
         catchup=False,
         tags=['ingestion', 'airbyte', 'dbt']) as dag:

    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt build'
    )

    for table_name, connection_uuid in AIRBYTE_JOB_IDS.items():
        
        task_sync = PythonOperator(
            task_id=f'sync_{table_name}',
            python_callable=trigger_airbyte_sync,
            op_kwargs={'connection_id': connection_uuid}
        )

        task_sync >> task_dbt_run
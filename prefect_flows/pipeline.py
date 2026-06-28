import os
import time
import requests
import subprocess
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect.variables import Variable
from dotenv import load_dotenv

load_dotenv()


def get_secret(name: str) -> str:
    return Secret.load(name).get()


def get_variable(name: str) -> str:
    return Variable.get(name)


def get_airbyte_token() -> str:
    client_id = get_secret("airbyte-client-id")
    client_secret = get_secret("airbyte-client-secret")

    response = requests.post(
        "https://api.airbyte.com/v1/applications/token",
        json={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials"
        }
    )
    response.raise_for_status()
    return response.json()["access_token"]


@task
def sync_airbyte(connection_id: str, name: str):
    logger = get_run_logger()
    logger.info(f"Starting Airbyte sync: {name}")

    token = get_airbyte_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(
        "https://api.airbyte.com/v1/jobs",
        headers=headers,
        json={"connectionId": connection_id, "jobType": "sync"}
    )
    response.raise_for_status()
    job_id = response.json()["jobId"]
    logger.info(f"Sync triggered — job ID: {job_id}")

    while True:
        status_response = requests.get(
            f"https://api.airbyte.com/v1/jobs/{job_id}",
            headers=headers
        )
        status_response.raise_for_status()
        status = status_response.json()["status"]
        logger.info(f"{name} sync status: {status}")

        if status == "succeeded":
            logger.info(f"{name} sync completed.")
            break
        elif status in ("failed", "cancelled"):
            raise Exception(f"{name} sync {status}.")

        time.sleep(10)


@task
def run_dbt(command: str):
    logger = get_run_logger()
    logger.info(f"Running: dbt {command}")

    databricks_token = get_secret("databricks-token")
    databricks_host = get_variable("databricks_host")
    databricks_http_path = get_variable("databricks_http_path")

    env = os.environ.copy()
    env["DATABRICKS_TOKEN"] = databricks_token
    env["DATABRICKS_HOST"] = databricks_host
    env["DATABRICKS_HTTP_PATH"] = databricks_http_path

    result = subprocess.run(
        f"dbt {command} --project-dir enterprise_lakehouse_dbt",
        shell=True,
        capture_output=True,
        text=True,
        env=env
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr)


@flow(name="enterprise-lakehouse-pipeline")
def pipeline():
    crm_id = get_secret("airbyte-crm-connection-id")
    erp_id = get_secret("airbyte-erp-connection-id")

    crm = sync_airbyte.submit(crm_id, "CRM")
    erp = sync_airbyte.submit(erp_id, "ERP")
    crm.result()
    erp.result()

    run_dbt("run --select silver")
    run_dbt("run --select gold")
    run_dbt("snapshot")
    run_dbt("test")


if __name__ == "__main__":
    pipeline()
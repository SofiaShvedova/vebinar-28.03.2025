import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rocket_local_report_alt",
    description="Download rocket pictures from primary and alternative sources with exception handling and reporting.",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
    catchup=False,
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="""
    curl -o /opt/airflow/data/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'
    """,
    dag=dag,
)

def _get_pictures():
    images_dir = "/opt/airflow/data/images"
    pathlib.Path(images_dir).mkdir(parents=True, exist_ok=True)

    report = {"successful_downloads": [], "failed_downloads": []}

    with open("/opt/airflow/data/launches.json", "r") as f:
        launches = json.load(f)

    for launch in launches["results"]:
        image_url = launch.get("image")
        alternative_image_url = launch.get("alternative_image")
        name = launch.get("name", "Unknown Launch")

        def download_image(url, filename):
            try:
                response = requests.get(url, stream=True, timeout=10)
                response.raise_for_status()
                target_file = f"{images_dir}/{filename}"
                with open(target_file, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded {url} to {target_file}")
                report["successful_downloads"].append({"name": name, "url": url})
                return True
            except requests_exceptions.RequestException as e:
                print(f"Failed to download {url} due to {e}")
                report["failed_downloads"].append({"name": name, "url": url, "reason": str(e)})
                return False

        if image_url:
            image_filename = image_url.split("/")[-1]
            if not download_image(image_url, image_filename):
                if alternative_image_url:
                    alt_image_filename = alternative_image_url.split("/")[-1]
                    if not download_image(alternative_image_url, alt_image_filename):
                        print(f"Failed to download both primary and alternative images for {name}")
                        report["failed_downloads"].append(
                            {"name": name, "reason": "Failed to download both primary and alternative images"}
                        )
                else:
                    print(f"No alternative image available for {name}")
                    report["failed_downloads"].append({"name": name, "reason": "No alternative image URL provided"})
        else:
            print(f"No image URL available for {name}")
            report["failed_downloads"].append({"name": name, "reason": "No image URL provided"})

    # Generating Report
    report_file = "/opt/airflow/data/download_report.json"
    with open(report_file, "w") as f:
        json.dump(report, f, indent=4)

    print(f"Download report saved to {report_file}")

get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)

def _notify():
    report_file = "/opt/airflow/data/download_report.json"
    try:
        with open(report_file, "r") as f:
            report_data = json.load(f)
        successful = len(report_data["successful_downloads"])
        failed = len(report_data["failed_downloads"])
        message = f"Download Report: Successful downloads: {successful}, Failed downloads: {failed}. Check {report_file} for details."
    except FileNotFoundError:
        message = f"Download report not found at {report_file}."
    print(message)

notify = PythonOperator(
    task_id="notify",
    python_callable=_notify,
    dag=dag,
)

download_launches >> get_pictures >> notify
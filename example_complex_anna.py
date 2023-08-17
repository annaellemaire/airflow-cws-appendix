"""
Example Airflow DAG that simulates a complex DAG.
"""
from __future__ import annotations

import pendulum
import time

from airflow import models
from airflow.models.baseoperator import chain
#from airflow.operators.bash import PythonOperator
from airflow.operators.python import PythonOperator


# Function to simulate task load
def simulate_task_load(cpu_iterations, io_sleep_time, random1, random2):
    for _ in range(cpu_iterations):
        _ = random1 * random2
    time.sleep(io_sleep_time)


with models.DAG(
    dag_id="example_complex_anna",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "example2", "example3"],
) as dag:
    # Create
    create_entry_group = PythonOperator(
        task_id="create_entry_group", 
        python_callable=simulate_task_load,
        op_args=[100000, 1, 0.35, 0.485])

    create_entry_group_result = PythonOperator(
        task_id="create_entry_group_result", 
        python_callable=simulate_task_load,
        op_args=[850000, 2, 0.355, 0.45])

    create_entry_group_result2 = PythonOperator(
        task_id="create_entry_group_result2",
        python_callable=simulate_task_load,
        op_args=[1000000, 1, 0.345, 0.85]
    )

    create_entry_gcs = PythonOperator(
        task_id="create_entry_gcs", python_callable=simulate_task_load,
        op_args=[1005000, 8, 0.55, 0.85575])

    create_entry_gcs_result = PythonOperator(
        task_id="create_entry_gcs_result", python_callable=simulate_task_load,
        op_args=[240000, 4, 0.75, 0.5]
    )

    create_entry_gcs_result2 = PythonOperator(
        task_id="create_entry_gcs_result2", python_callable=simulate_task_load,
        op_args=[1700000, 1, 0.735, 0.5485]
    )

    create_tag = PythonOperator(task_id="create_tag", python_callable=simulate_task_load,
        op_args=[200000, 7, 0.345, 0.485])

    create_tag_result = PythonOperator(task_id="create_tag_result", python_callable=simulate_task_load,
        op_args=[180000, 6, 0.356, 0.5])

    create_tag_result2 = PythonOperator(task_id="create_tag_result2", python_callable=simulate_task_load,
        op_args=[900000, 10, 0.35, 0.485])

    create_tag_template = PythonOperator(task_id="create_tag_template", python_callable=simulate_task_load,
        op_args=[5600000, 2, 0.355, 0.4])

    create_tag_template_result = PythonOperator(
        task_id="create_tag_template_result", python_callable=simulate_task_load,
        op_args=[2900000, 9, 0.5, 0.4845]
    )

    create_tag_template_result2 = PythonOperator(
        task_id="create_tag_template_result2", python_callable=simulate_task_load,
        op_args=[2200000, 1, 0.845675, 0.485]
    )

    create_tag_template_field = PythonOperator(
        task_id="create_tag_template_field", python_callable=simulate_task_load,
        op_args=[1200000, 8, 0.358, 0.5]
    )

    create_tag_template_field_result = PythonOperator(
        task_id="create_tag_template_field_result", python_callable=simulate_task_load,
        op_args=[700000, 2, 0.45, 0.7]
    )

    create_tag_template_field_result2 = PythonOperator(
        task_id="create_tag_template_field_result2", python_callable=simulate_task_load,
        op_args=[1080000, 3, 0.5, 0.48585]
    )

    # Delete
    delete_entry = PythonOperator(task_id="delete_entry", python_callable=simulate_task_load,
        op_args=[1200000, 4, 0.587, 0.45])
    create_entry_gcs >> delete_entry

    delete_entry_group = PythonOperator(task_id="delete_entry_group", python_callable=simulate_task_load,
        op_args=[2800000, 5, 0.5, 0.985])
    create_entry_group >> delete_entry_group

    delete_tag = PythonOperator(task_id="delete_tag", python_callable=simulate_task_load,
        op_args=[500000, 6, 0.3, 0.5])
    create_tag >> delete_tag

    delete_tag_template_field = PythonOperator(
        task_id="delete_tag_template_field", python_callable=simulate_task_load,
        op_args=[800000, 7, 0.3455, 0.885]
    )

    delete_tag_template = PythonOperator(task_id="delete_tag_template", python_callable=simulate_task_load,
        op_args=[1900000, 8, 0.17, 0.76])

    # Get
    get_entry_group = PythonOperator(task_id="get_entry_group", python_callable=simulate_task_load,
        op_args=[2100000, 1, 0.35, 0.485])

    get_entry_group_result = PythonOperator(
        task_id="get_entry_group_result", python_callable=simulate_task_load,
        op_args=[47800000, 2, 0.5, 0.48589]
    )

    get_entry = PythonOperator(task_id="get_entry", python_callable=simulate_task_load,
        op_args=[17800000, 3, 0.3548, 0.95])

    get_entry_result = PythonOperator(task_id="get_entry_result", python_callable=simulate_task_load,
        op_args=[1070000, 9, 0.835, 0.85])

    get_tag_template = PythonOperator(task_id="get_tag_template", python_callable=simulate_task_load,
        op_args=[1400000, 4, 0.315, 0.1485])

    get_tag_template_result = PythonOperator(
        task_id="get_tag_template_result", python_callable=simulate_task_load,
        op_args=[1870000, 5, 0.31, 0.7985]
    )

    # List
    list_tags = PythonOperator(task_id="list_tags", python_callable=simulate_task_load,
        op_args=[7900000, 6, 0.45, 0.58])

    list_tags_result = PythonOperator(task_id="list_tags_result", python_callable=simulate_task_load,
        op_args=[1100000, 7, 0.345, 0.1])

    # Lookup
    lookup_entry = PythonOperator(task_id="lookup_entry", python_callable=simulate_task_load,
        op_args=[1200000, 8, 0.35, 0.485])

    lookup_entry_result = PythonOperator(task_id="lookup_entry_result", python_callable=simulate_task_load,
        op_args=[100000, 9, 0.735, 0.85])

    # Rename
    rename_tag_template_field = PythonOperator(
        task_id="rename_tag_template_field", python_callable=simulate_task_load,
        op_args=[800000, 1, 0.356, 0.45]
    )

    # Search
    search_catalog = PythonOperator(task_id="search_catalog", python_callable=simulate_task_load,
        op_args=[200000, 1, 0.355, 0.4785])

    search_catalog_result = PythonOperator(
        task_id="search_catalog_result", python_callable=simulate_task_load,
        op_args=[500000, 5, 0.3675, 0.5]
    )

    # Update
    update_entry = PythonOperator(task_id="update_entry", python_callable=simulate_task_load,
        op_args=[1070000, 1, 0.365, 0.4856])

    update_tag = PythonOperator(task_id="update_tag", python_callable=simulate_task_load,
        op_args=[400000, 7, 0.355, 0.485])

    update_tag_template = PythonOperator(task_id="update_tag_template", python_callable=simulate_task_load,
        op_args=[900000, 3, 0.355, 0.14785])

    update_tag_template_field = PythonOperator(
        task_id="update_tag_template_field", python_callable=simulate_task_load,
        op_args=[1200000, 8, 0.365, 0.1818]
    )

    # Create
    create_tasks = [
        create_entry_group,
        create_entry_gcs,
        create_tag_template,
        create_tag_template_field,
        create_tag,
    ]
    chain(*create_tasks)

    create_entry_group >> delete_entry_group
    create_entry_group >> create_entry_group_result
    create_entry_group >> create_entry_group_result2

    create_entry_gcs >> delete_entry
    create_entry_gcs >> create_entry_gcs_result
    create_entry_gcs >> create_entry_gcs_result2

    create_tag_template >> delete_tag_template_field
    create_tag_template >> create_tag_template_result
    create_tag_template >> create_tag_template_result2

    create_tag_template_field >> delete_tag_template_field
    create_tag_template_field >> create_tag_template_field_result
    create_tag_template_field >> create_tag_template_field_result2

    create_tag >> delete_tag
    create_tag >> create_tag_result
    create_tag >> create_tag_result2

    # Delete
    delete_tasks = [
        delete_tag,
        delete_tag_template_field,
        delete_tag_template,
        delete_entry_group,
        delete_entry,
    ]
    chain(*delete_tasks)

    # Get
    create_tag_template >> get_tag_template >> delete_tag_template
    get_tag_template >> get_tag_template_result

    create_entry_gcs >> get_entry >> delete_entry
    get_entry >> get_entry_result

    create_entry_group >> get_entry_group >> delete_entry_group
    get_entry_group >> get_entry_group_result

    # List
    create_tag >> list_tags >> delete_tag
    list_tags >> list_tags_result

    # Lookup
    create_entry_gcs >> lookup_entry >> delete_entry
    lookup_entry >> lookup_entry_result

    # Rename
    create_tag_template_field >> rename_tag_template_field >> delete_tag_template_field

    # Search
    chain(create_tasks, search_catalog, delete_tasks)
    search_catalog >> search_catalog_result

    # Update
    create_entry_gcs >> update_entry >> delete_entry
    create_tag >> update_tag >> delete_tag
    create_tag_template >> update_tag_template >> delete_tag_template
    create_tag_template_field >> update_tag_template_field >> rename_tag_template_field

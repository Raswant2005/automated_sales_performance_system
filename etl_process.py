from dag_functions import extract_data_from_aws,transform_data,generate_insights,transform_to_sql
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.utils.email import send_email 



def send_success_email(context=None):
    try:
        subject = f"Airflow Test Task succeeded!(Zaalima)"
        body = f"PowerBI dashboard is ready!!. You can access through this link: {'**************'}"
        

        if context:
            send_email(to='**********@gmail.com', subject=subject, html_content=body, context=context)
        else:
            send_email(to='************@gmail.com', subject=subject, html_content=body)
    except Exception as e:
        print("email send to [************@gmail.com]!!")


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'depends_on_past': False,
    'on_success_callback': send_success_email

} 

dag = DAG(
    'zaalima_project_dag',
    default_args = default_args,
    start_date = datetime(2025,4,24),
    schedule = '@daily',
    catchup = False
)

extract_datas = PythonOperator(
    task_id = 'extract_data',
    python_callable = extract_data_from_aws,
    dag = dag
)

transform_datas = PythonOperator(
    task_id = 'transforming_data',
    python_callable = transform_data,
    dag = dag
)

generate_insights_viz = PythonOperator(
    task_id = 'generate_insights_viz',
    python_callable = generate_insights,
    dag = dag
)

sql_transform = PythonOperator(
    task_id = 'transform_to_sql',
    python_callable = transform_to_sql,
    dag = dag
)

email_powerbi_task = PythonOperator(
    task_id='run_etl_task',
    python_callable=send_success_email,
    op_kwargs={"context": "{{ task_instance.xcom_pull() }}"},
    dag=dag
)




extract_datas >> transform_datas >> generate_insights_viz >> sql_transform >> email_powerbi_task 

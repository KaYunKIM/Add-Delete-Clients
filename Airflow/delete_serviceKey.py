from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from datetime import timedelta, datetime
import yaml

def delete_from_clients(**kwargs):
    serviceKey = kwargs['dag_run'].conf.get('serviceKey')
    print('serviceKey: ', serviceKey)

    with open('/data/airflow/G2R12N-CONF/clients.yml', encoding="utf-8") as f:
        content = f.read()

    parsed_data = yaml.load(content, Loader=yaml.FullLoader)
    client_name = parsed_data[serviceKey]["name"]

    serviceKeys = [x for x in content.split("\n\n")]
    new_clients = [x for x in serviceKeys if serviceKey not in x]

    with open('/data/airflow/G2R12N-CONF/clients.yml', 'w', encoding="utf-8") as outfile:
        outfile.write('\n\n'.join(new_clients))

    return client_name


def trigger_delete_dags(**kwargs):
    serviceKey = kwargs['dag_run'].conf.get('serviceKey')

    with open('/data/airflow/G2R12N-CONF/clients_copy.yml', encoding="utf-8") as f:
        content = f.read()

    parsed_data = yaml.load(content, Loader=yaml.FullLoader)

    for type in parsed_data[serviceKey]:
        if type == "name":
            client_name = parsed_data[serviceKey]["name"]
        elif type == "purchase" or type == "rfm":
            if parsed_data[serviceKey][type]['use'] == 'Y':
                delete_seg_dags = BashOperator(
                    task_id="delete_{}_{}".format(type, client_name),
                    bash_command="cd /data/pem && ssh -i 'groobeeds.pem' ec2-user@ec2-3-34-48-165.ap-northeast-2.compute.amazonaws.com /home/ec2-user/.local/bin/airflow dags delete -y ml.{}_{}_service_v2.0".format(type, client_name),
                    queue="keyword_train",
                    dag=dag,
                )
                delete_seg_dags.execute(dict()) 
        else:
            if parsed_data[serviceKey][type]['use'] == 'Y':
                delete_rec_dags = BashOperator(
                    task_id="delete_{}_{}".format(type, client_name),
                    bash_command="airflow delete_dag -y {}_{}_v2.0".format(type, client_name),
                    queue="keyword_train",
                    dag=dag,
                )
                delete_rec_dags.execute(dict()) 

    return


def slack_fail_alert(context):
    alert = SlackWebhookHook(
        http_conn_id="slack",
        channel="#datascience",
        username="airflow_bot",
        message="""
                :red_circle: Task Failed. 
                *Task*: {task}  
                *Dag*: {dag} 
                *Execution Time*: {exec_date}  
                # modify base_url param of airflow.cfg
                """.format(
            task=context.get("task_instance").task_id,
            dag=context.get("task_instance").dag_id,
            ti=context.get("task_instance"),
            exec_date=context.get("execution_date"),
            log_url=context.get("task_instance").log_url,
        ),
    )
    return alert.execute()


def slack_success_alert(context):
    alert = SlackWebhookHook(
        http_conn_id="slack",
        channel="#datascience",
        username="airflow_bot",
        message="""
                :green_heart: Task Successed. 
                *Task*: {task}  
                *Dag*: {dag} 
                *Execution Time*: {exec_date}  
                # modify base_url param of airflow.cfg
                """.format(
            task=context.get("task_instance").task_id,
            dag=context.get("task_instance").dag_id,
            ti=context.get("task_instance"),
            exec_date=context.get("execution_date"),
            log_url=context.get("task_instance").log_url,
        ),
    )
    return alert.execute()


# dag
dag_name = "delete_serviceKey"

default_args = {
    "owner": "airflow",  # owner name of the DAG
    "depends_on_past": False,  # whether to rely on previous task status
    "start_date": datetime(2021, 4, 30),  # start date of task instance
    # "on_success_callback": slack_success_alert,
    "on_failure_callback": slack_fail_alert,
    "retries": 1,  # retry the task once, if it fails
    "retry_delay": timedelta(minutes=3),  # after waiting for 3 minq
}

with DAG(dag_name, default_args=default_args, schedule_interval=None) as dag:

    # serviceKey_xcom_push = BashOperator(
    #     task_id = "serviceKey_xcom_push",
    #     bash_command = 'echo "{{ dag_run.conf["serviceKey"]}}" ',
    #     queue="keyword_train",
    #     provide_context=True,
    #     xcom_push=True,
    #     dag=dag,
    # )
    
    git_pull_CONF = BashOperator(
        task_id="git_pull_CONF",
        bash_command="cd /data/airflow/G2R12N-CONF && git fetch --all && git reset --hard origin/master",
        queue="keyword_train",
        dag=dag,
    )

    copy_clients = BashOperator(
        task_id="copy_clients",
        bash_command="cd /data/airflow/G2R12N-CONF && cp clients.yml clients_copy.yml",
        queue="keyword_train",
        dag=dag,
    )

    delete_serviceKey = PythonOperator(
        task_id="delete_serviceKey",
        python_callable=delete_from_clients,
        queue="keyword_train",
        provide_context=True,
        dag=dag,
    )

    update_clients = BashOperator(
        task_id="update_clients",
        bash_command="cd /data/airflow/G2R12N-CONF && git add clients.yml && git commit -m 'Delete {{ task_instance.xcom_pull('delete_serviceKey', key='return_value') }}' && git push origin master",
        queue="keyword_train",
        dag=dag,
    )

    delete_dags = PythonOperator(
        task_id="delete_dags",
        python_callable=trigger_delete_dags,
        queue="keyword_train",
        provide_context=True,
        dag=dag,
    )

    git_pull_CONF >> copy_clients >> delete_serviceKey >> update_clients >> delete_dags
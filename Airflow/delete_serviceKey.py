def delete_from_clients(**kwargs):
    serviceKey = kwargs['dag_run'].conf.get('serviceKey')
    print('serviceKey: ', serviceKey)

    with open('clients.yml', encoding="utf-8") as f:
        content = f.read()

    parsed_data = yaml.load(content, Loader=yaml.FullLoader)
    client_name = parsed_data[serviceKey]["name"]

    serviceKeys = [x for x in content.split("\n\n")]
    new_clients = [x for x in serviceKeys if serviceKey not in x]

    with open('clients.yml', 'w', encoding="utf-8") as outfile:
        outfile.write('\n\n'.join(new_clients))

    return client_name


def trigger_delete_dags(**kwargs):
    serviceKey = kwargs['dag_run'].conf.get('serviceKey')

    with open('clients_copy.yml', encoding="utf-8") as f:
        content = f.read()

    parsed_data = yaml.load(content, Loader=yaml.FullLoader)

    for type in parsed_data[serviceKey]:
        if type == "name":
            client_name = parsed_data[serviceKey]["name"]
        elif type == "purchase" or type == "rfm":
            if parsed_data[serviceKey][type]['use'] == 'Y':
                delete_seg_dags = BashOperator(
                    task_id="delete_{}_{}".format(type, client_name),
                    bash_command="cd /data/pem && ssh -i ... /home/ec2-user/.local/bin/airflow dags delete -y ml.{}_{}_service_v2.0".format(type, client_name),
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

# dag
with DAG(dag_name, default_args=default_args, schedule_interval=None) as dag:
    
    git_pull_CONF = BashOperator(
        task_id="git_pull_CONF",
        bash_command="git fetch --all && git reset --hard origin/master",
        queue="keyword_train",
        dag=dag,
    )

    copy_clients = BashOperator(
        task_id="copy_clients",
        bash_command="cp clients.yml clients_copy.yml",
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
        bash_command="git add clients.yml && git commit -m 'Delete {{ task_instance.xcom_pull('delete_serviceKey', key='return_value') }}' && git push origin master",
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

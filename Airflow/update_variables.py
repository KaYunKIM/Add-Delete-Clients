def main():
    # Set DagTriggers
    dag_triggers = get_dag_trigger(urls, user_name, pwd, deploy_airflow)
    
    # Set DagHandlers(unpause)
    dag_handlers = get_dag_handler(urls, user_name, pwd, deploy_airflow)

    # Arrange airflow variables
    for airflow in deploy_airflow:
        if airflow == "rec":
            arrange_var_rec_airflow(dag_handlers, dag_triggers)
        elif airflow == "seg":
            arrange_var_seg_airflow(dag_handlers, dag_triggers)

    logging.info("Success!!!")

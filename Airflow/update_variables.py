import yaml
import argparse
import logging
import os
from utils.api import *


def main():
    # Get urls
    urls = get_urls(api, args.net, deploy_airflow)

    # Set VariableHandlers
    var_handlers = get_var_handler(urls, user_name, pwd, deploy_airflow)

    # Set DagTriggers
    dag_triggers = get_dag_trigger(urls, user_name, pwd, deploy_airflow)
    
    # Set DagHandlers(unpause)
    dag_handlers = get_dag_handler(urls, user_name, pwd, deploy_airflow)

    # Fetch Dag List
    # dag_list = get_dag_list(urls, user_name, pwd, deploy_airflow)

    # Define sets to check the client using each model.
    client_vars, name2servicekey = get_rec_ml_seg_clients(clients)

    # Get airflow variables(key, value)
    airflow_vars = get_airflow_variables(var_handlers)

    # Arrange airflow variables
    for airflow in deploy_airflow:
        if airflow == "rec":
            arrange_var_rec_airflow(rec_default_vars, client_vars, airflow_vars, name2servicekey, var_handlers, dag_handlers, dag_triggers, clients)
        elif airflow == "seg":
            arrange_var_seg_airflow(seg_default_vars, client_vars, airflow_vars, name2servicekey, var_handlers, dag_handlers, dag_triggers)
        elif airflow == "test":
            arrange_var_test_airflow(rec_default_vars, client_vars, airflow_vars, name2servicekey, var_handlers, dag_handlers, dag_triggers)

    logging.info("Success!!!")


if __name__ == "__main__":
    # argument parse
    parser = argparse.ArgumentParser()
    parser.add_argument("--net", choices=["private", "public"], required=True, type=str)
    parser.add_argument("--env", choices=["prod", "dev"], required=True, type=str)
    parser.add_argument("--user_name", required=True, type=str)
    parser.add_argument("--password", required=True, type=str)
    args = parser.parse_args()

    # load api config
    clients = yaml.safe_load(open("clients.yml"))
    api = yaml.safe_load(open("api.yml"))
    rec_default_vars = api["default"]["rec"]
    seg_default_vars = api["default"]["seg"]
    user_name = args.user_name
    pwd = args.password
    deploy_airflow = choice_deploy_airflow(args.env)

    # set logger
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y/%m/%d %H:%M:%S", level=logging.INFO)

    # run scripts
    main()

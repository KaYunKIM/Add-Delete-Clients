# unpause DAG
class DagHandler:
    # def unpause(self, key, value, dag_list):
    def unpause(self, key, value, type):
        # logging.info(dag_list)
        logging.info('unpause fnc started...')
        dag_name = dict()

        dag_name['rec'] = ['keyword', 'statistic', 'statistic_real', 'ml', 'dl']
        dag_name['seg'] = ['purchase', 'rfm']

        clients = open("clients.yml")
        parsed_data = yaml.load(clients, Loader=yaml.FullLoader)

        if type == 'rec':
            for rec_dag in dag_name[type]:
                if parsed_data[value["service_key"]][rec_dag]["use"] == 'Y':
                    url = f"{self.url}&dag_id={rec_dag}_{key}_v2.0"
                    res = requests.get(url, headers=self.headers)

        elif type == 'seg':
            for seg_dag in dag_name[type]:
                if parsed_data[value["service_key"]][seg_dag]["use"] == 'Y':
                    url = f"{self.url}&dag_id={seg_dag}_{key}_service_v2.0"
                    res = requests.get(url, headers=self.headers)
        
        else:
            url = f"{self.url}&dag_id={key}"
    
        logging.info(f"'{key} {type}' DAGs has been unpaused")
        return res, res.status_code

     
class DagTrigger:
    def trigger_mongodb_index(self, type):
        # create index in mongoDB
        mongodb_index_url = f"{self.url}&dag_id=mongodb_create_col_idx_{type}"
        mongodb_index_res = requests.get(mongodb_index_url, headers=self.headers)
                    
        return mongodb_index_res, mongodb_index_res.status_code


def get_dag_trigger(urls, user_name, pwd, deploy_airflow):
    dag_triggers = dict()
    for airflow in deploy_airflow:
        access_token = AuthorizationHandler(urls[airflow]["token"]).get_access_token(user_name, pwd)
        dag_triggers[airflow] = DagTrigger(access_token, urls[airflow]["trigger_dag"])
    return dag_triggers


def get_dag_handler(urls, user_name, pwd, deploy_airflow):
    dag_handlers = dict()
    for airflow in deploy_airflow:
        access_token = AuthorizationHandler(urls[airflow]["token"]).get_access_token(user_name, pwd)
        dag_handlers[airflow] = DagHandler(access_token, urls[airflow]["unpause"])
    return dag_handlers


def arrange_var_rec_airflow(dag_triggers, dag_handlers):
    # add variable to rec airflow
    for client_name in only_in_clients:
        # trigger DAG
        dag_triggers["rec"].trigger_mongodb_index("rec")
        # unpause DAG
        dag_handlers["rec"].unpause(client_name, json.loads(value), "rec")

    # delete variable from rec airflow
    for client_name in only_in_airflow:
        var_handlers["rec"].delete_variable(client_name)


def arrange_var_seg_airflow(dag_triggers, dag_handlers):
    # add variable to seg airflow
    for client_name in only_in_clients:
        # trigger DAG
        dag_triggers["seg"].trigger_mongodb_index("seg")
        # unpause DAG
        dag_handlers["seg"].unpause(client_name, json.loads(value), "seg")

    # delete variable from seg airflow
    for client_name in only_in_airflow:
        var_handlers["seg"].delete_variable(client_name)

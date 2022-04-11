import requests
import json
import logging
import yaml
import time


class AuthorizationHandler:
    def __init__(self, url):
        self.headers = {"Content-Type": "application/json"}
        self.url = url

    def get_access_token(self, username, password):
        try:
            token_res = requests.post(
                self.url,
                headers=self.headers,
                data=json.dumps({"username": username, "password": password, "refresh": True, "provider": "db"}),
            )
            logging.info(f"Access token for '{self.url}' was created !!!")
            return json.loads(token_res.text)["access_token"]
        except Exception as e:
            raise Exception(e)


class VariableHandler:
    def __init__(self, access_token, url):
        self.url = url
        self.headers = {"Authorization": f"Bearer {access_token}"}
        logging.info(f"Variable handler for '{url}' was created !!!")

    def set_variable(self, key, value):
        """
        Set variables by key and value.

        Args:
            key (str): key for variable
            value (str): value corresponding key
        """
        url = f"{self.url}&cmd=set&key={key}&value={value}"
        res, status_code, output = self.get_request(url)
        logging.info(f"'{key}={value}' was created in {self.url}")

    def delete_variable(self, key):
        """
        Delete key and value.
            We can't distinguish between the case with and without a key in variables.
            This is because both return empty string('').

        Args:
            key (str): key in variables
        """
        url = f"{self.url}&delete={key}"
        res, status_code, output = self.get_request(url)
        logging.info(f"'{key}' was deleted.")

    def get_variable(self, key, default="N"):
        """
        Get value by key. If key doesn't exist in variables, return str({default})

        Args:
            key (str): key in variables
            default (str): Default value returned if key does not exist

        Returns:
            output (str): key's value or {default}
        """
        url = f"{self.url}&get={key}&default={default}"
        res, status_code, output = self.get_request(url)
        if output == "N":
            logging.info(f"'{key}' was not in variables.")
        else:
            logging.info(f"'{key}' was fetched.")
        return output

    def get_all_variables_keys(self):
        """
        Get all keys in variables.

        Returns:
            keys (list): all keys in variables.
        """
        res, status_code, output = self.get_request(self.url)
        keys = output.split("\n")
        logging.info(f"All keys for '{self.url}' was fetched.")
        return keys

    def get_request(self, url):
        """
        Request to airflow webserver REST API by get method.
        Supports both http GET and POST methods.

        Args:
            url (string): url containing informations

        Returns:
            res: response object
            status_code (str): status code of response
            output (str): stdout of response
        """
        try:
            res = requests.get(url, headers=self.headers)
            status_code = res.status_code
            output = json.loads(res.text)["output"]["stdout"].strip()
            return res, status_code, output
        except Exception as e:
            raise Exception(f"Error in get_request function: {e}")

# unpause DAG
class DagHandler:
    def __init__(self, access_token, url):
        self.url = url
        self.headers = {"Authorization": f"Bearer {access_token}"}
        logging.info(f"DAG handler for '{url}' was created !!!")

    # def unpause(self, key, value, dag_list):
    def unpause(self, key, value, type):
        """
        Trigger DAG by key.
        Args:
            key (str): key for variable
        """
        # logging.info(dag_list)
        logging.info('unpause fnc started.....')
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


        # for dag in parsed_data[value["service_key"]].keys():
        #     if dag == "name":
        #         pass
        #     elif dag in ['keyword', 'statistic', 'statistic_real', 'ml', 'dl']:
        #         logging.info(f"---------print '{dag}' name-------------")
        #         logging.info(self.url)
        #         if 
        #         # if '172.16.167.16' in self.url and parsed_data[value["service_key"]][dag]["use"] == 'Y':
        #         # if '13.209.26.92' in self.url and parsed_data[value["service_key"]][dag]["use"] == 'Y':
        #             url = f"{self.url}&dag_id={dag_name[dag][0]}_{key}_{dag_name[dag][1]}"
        #             logging.info(url)
        #     else:
        #         logging.info(f"---------print '{dag}' name-------------")
        #         logging.info(self.url)
        #         if '172.16.164.174' in self.url and  parsed_data[value["service_key"]][dag]["use"] == 'Y':
        #         # if '3.34.48.165' in self.url and  parsed_data[value["service_key"]][dag]["use"] == 'Y':
        #             url = f"{self.url}&dag_id={dag_name[dag][0]}_{key}_{dag_name[dag][1]}"
        #             logging.info(url)
            
        #     if url:
        #         logging.info('if url....') 
        #         res = requests.get(url, headers=self.headers)
        #         status_code = res.status_code
        #         print(res.text)
        #         # logging.info(res, status_code)
        #         # output = json.loads(res.text)["output"]["stdout"].strip()

        #         logging.info(f"'{dag_name[dag][0]}_{key}_{dag_name[dag][1]}' DAG has been unpaused")
        # return res, status_code


# class DagList:
#     def __init__(self, access_token, url):
#         self.url = url
#         self.headers = {"Authorization": f"Bearer {access_token}"}
#         logging.info(f"get DAG lists Finished !!!")

#     def dag_list(self, key, value, get_dag_list):
#         res = requests.get(self.url, headers=self.headers)
#         status_code = res.status_code
#         logging.info(res, status_code)
#         output = json.loads(res.text)["output"]["stdout"].strip()

#         logging.info(f"DAG list has been fetched")
#         return res, status_code, output


class DagTrigger:
    def __init__(self, access_token, url):
        self.url = url
        self.headers = {"Authorization": f"Bearer {access_token}"}
        logging.info(f"DAG handler for '{url}' was created !!!")

    def trigger_mongodb_index(self, type):
        """
        Trigger DAG by key.
        Args:
            key (str): key for variable
        """
        # create index in mongoDB
        mongodb_index_url = f"{self.url}&dag_id=mongodb_create_col_idx_{type}"
        mongodb_index_res = requests.get(mongodb_index_url, headers=self.headers)
                    
        return mongodb_index_res, mongodb_index_res.status_code


def get_urls(api, net, deploy_airflow):
    urls = dict()
    for airflow in deploy_airflow:
        sub_dict = dict()
        for endpoint in api["endpoints"]:
            sub_dict[endpoint] = api["airflows"][airflow][net] + api["endpoints"][endpoint]
        urls[airflow] = sub_dict
    return urls


def get_var_handler(urls, user_name, pwd, deploy_airflow):
    var_handlers = dict()
    for airflow in deploy_airflow:
        access_token = AuthorizationHandler(urls[airflow]["token"]).get_access_token(user_name, pwd)
        var_handlers[airflow] = VariableHandler(access_token, urls[airflow]["variable"])
    return var_handlers


# def get_dag_list(urls, user_name, pwd, deploy_airflow):
#     dag_list = dict()
#     for airflow in deploy_airflow:
#         access_token = AuthorizationHandler(urls[airflow]["token"]).get_access_token(user_name, pwd)
#         dag_list[airflow] = DagList(access_token, urls[airflow]["list_dags"])
#     return dag_list


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


def choice_deploy_airflow(env="dev"):
    if env == "prod":
        return ["rec", "seg"]
    elif env == "dev":
        return ["test"]


def get_rec_ml_seg_clients(clients):
    rec_clients, ml_clients, seg_clients = set(), set(), set()
    name2servicekey = dict()

    for service_key, service_key_info in clients.items():
        # get client_name
        client_name = service_key_info.pop("name")
        name2servicekey[client_name] = service_key

        # check if client use recommendation, ml, segmentation models
        use_rec, use_ml, use_seg = False, False, False
        for algo, algo_info in service_key_info.items():

            # if client use purchase and rfm model, convert 'use_seg' to True
            if algo in ["purchase", "rfm"]:
                use_seg = use_seg or (algo_info["use"] == "Y")

            # if client use other models, convert 'use_rec' to True
            else:
                if algo == "ml":
                    use_ml = use_ml or (algo_info["use"] == "Y")
                use_rec = use_rec or (algo_info["use"] == "Y")

        # add client name to sets
        if use_rec:
            rec_clients.add(client_name)
        if use_ml:
            ml_clients.add(client_name)
        if use_seg:
            seg_clients.add(client_name)

    client_vars = {"rec": {"rec": rec_clients, "ml": ml_clients}, "seg": seg_clients}
    return client_vars, name2servicekey


def get_airflow_variables(var_handlers):
    airflow_vars = dict()
    for airflow, var_handler in var_handlers.items():
        airflow_vars[airflow] = set(var_handler.get_all_variables_keys())
    return airflow_vars


def arrange_var_rec_airflow(default_vars, client_vars, airflow_vars, name2servicekey, var_handlers, dag_handlers, dag_triggers, client_yaml):
    ml_vars = client_vars["rec"]["ml"]
    client_vars = client_vars["rec"]["rec"]
    airflow_vars = airflow_vars["rec"]

    only_in_clients = client_vars.difference(airflow_vars)
    only_in_airflow = airflow_vars.difference(client_vars).difference(set([k for k in default_vars]))

    # add default vars to rec airflow
    for k, v in default_vars.items():
        if k == "mongodb":
            v = json.dumps(v)
        var_handlers["rec"].set_variable(k, v)

    # add variable to rec airflow
    for client_name in only_in_clients:
        service_key = name2servicekey[client_name]
        value = {"service_key": service_key}
        if client_name in ml_vars:
            value.update({"ml_newbie_yn": "N", "preference_based_data": client_yaml[service_key]["ml"]["preference_based_data"]})
        value = json.dumps(value)
        var_handlers["rec"].set_variable(client_name, value)
        # trigger DAG
        dag_triggers["rec"].trigger_mongodb_index("rec")
        # unpause DAG
        dag_handlers["rec"].unpause(client_name, json.loads(value), "rec")

    # delete variable from rec airflow
    for client_name in only_in_airflow:
        var_handlers["rec"].delete_variable(client_name)


def arrange_var_seg_airflow(default_vars, client_vars, airflow_vars, name2servicekey, var_handlers, dag_handlers, dag_triggers):
    client_vars = client_vars["seg"]
    airflow_vars = airflow_vars["seg"]

    only_in_clients = client_vars.difference(airflow_vars)
    only_in_airflow = airflow_vars.difference(client_vars).difference(set([k for k in default_vars]))

    # add default vars to rec airflow
    for k, v in default_vars.items():
        if k == "mongodb":
            v = json.dumps(v)
        var_handlers["seg"].set_variable(k, v)

    # add variable to seg airflow
    for client_name in only_in_clients:
        value = json.dumps({"service_key": name2servicekey[client_name]})
        var_handlers["seg"].set_variable(client_name, value)
        # trigger DAG
        dag_triggers["seg"].trigger_mongodb_index("seg")
        # unpause DAG
        dag_handlers["seg"].unpause(client_name, json.loads(value), "seg")

    # delete variable from seg airflow
    for client_name in only_in_airflow:
        var_handlers["seg"].delete_variable(client_name)


def arrange_var_test_airflow(default_vars, client_vars, airflow_vars, name2servicekey, var_handlers, dag_handlers, dag_triggers):
    ml_vars = client_vars["rec"]["ml"]
    client_vars = client_vars["rec"]["rec"]
    airflow_vars = airflow_vars["test"]

    only_in_clients = client_vars.difference(airflow_vars)
    only_in_airflow = airflow_vars.difference(client_vars).difference(set([k for k in default_vars]))

    # add default vars to rec airflow
    for k, v in default_vars.items():
        if k == "mongodb":
            v = json.dumps(v)
        var_handlers["test"].set_variable(k, v)

    # add variable to rec airflow
    for client_name in only_in_clients:
        value = {"service_key": name2servicekey[client_name]}
        if client_name in ml_vars:
            value.update({"ml_newbie_yn": "N", "preference_based_data": ""})
        value = json.dumps(value)
        var_handlers["test"].set_variable(client_name, value)
        # trigger DAG
        dag_triggers["test"].trigger_mongodb_index("test")
        # unpause DAG
        dag_handlers["test"].unpause(client_name, json.loads(value), "test")

    # delete variable from rec airflow
    for client_name in only_in_airflow:
        var_handlers["test"].delete_variable(client_name)


logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y/%m/%d %H:%M:%S", level=logging.INFO)

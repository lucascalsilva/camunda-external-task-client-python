import requests
import json
import time
import uuid
import sys
import logging


class external_task_client:
    def __init__(self, url, workerid=uuid.uuid1()):
        self.url = str(url)
        self.workerid = str(workerid)

    def fetchtasks(self, topic, lockDuration=1000, longPolling=5000, maxTasks=1, usePriority=True):
        # Define the endpoint for fetch and lock
        endpoint = self.url + '/external-task/fetchAndLock'
        # Define unique ID for the worker
        workerid = self.workerid

        # Define the Json for the Request
        fetch_and_lock_req = {'workerId': workerid,
                              'maxTasks': maxTasks,
                              'usePriority': usePriority,
                              'asyncResponseTimeout': longPolling,
                              'topics':
                              [{'topicName': topic,
                                'lockDuration': lockDuration
                                }]
                              }

        logging.info(f'Starting fetch and lock. Request: {fetch_and_lock_req}')

        response = '[]'

        while response == '[]':
            try:
                logging.info(f'Polling topic {topic}...')
                post_response = requests.post(
                    endpoint, json=fetch_and_lock_req)
                self.validate_requests(post_response)
                response = post_response.text
            except:
                logging.error(f'Couldnt fetch and lock.')
                logging.error(f'Error: {sys.exc_info()}')
            finally:
                if response == '[]':
                    time.sleep(5)

        return json.loads(response)

    # Complete Call
    def complete(self, taskid, **kwargs):
        endpoint = str(self.url) + '/external-task/' + taskid + '/complete'

        # puts the variables from the dictonary into the nested format for the json response
        variables_for_request = {}
        for key, val in kwargs.items():
            variables_for_request.update({key: val})

        complete_req = {'workerId': self.workerid,
                        'variables': variables_for_request}

        try:
            resp = requests.post(endpoint, json=complete_req)
            self.validate_requests(resp)
            logging.info(f'Task {taskid} completed...')
        except:
            logging.error(f'Failure on complete task {taskid}...')
            logging.error(f'Error: {sys.exc_info()}')

    # BPMN Error

    def error(self, taskid, bpmn_error, error_message='not defined', **kwargs):
        endpoint = str(self.url) + '/external-task/' + taskid + '/bpmnError'

        variables_for_request = {}
        for key, val in kwargs.items():
            variables_for_request.update({key: val})

        response = {
            'workerId': self.workerid,
            'errorCode': bpmn_error,
            'errorMessage': error_message,
            'variables': variables_for_request
        }

        try:
            resp = requests.post(endpoint, json=response)
            self.validate_requests(resp)
            logging.info(f'Task {taskid} error sent...')
        except:
            logging.error(f'Failure on sending error to task {taskid}...')
            logging.error(f'Error: {sys.exc_info()}')

    # Create an incident

    def fail(self, taskid, error_message, retries=0, retry_timeout=0):
        endpoint = str(self.url) + '/external-task/' + taskid + '/failure'

        request = {
            'workerId': self.workerid,
            'errorMessage': error_message,
            'retries': retries,
            'retryTimeout': retry_timeout}

        try:
            resp = requests.post(endpoint, json=request)
            self.validate_requests(resp)
            logging.info(f'Task {taskid} failure sent...')
        except:
            logging.error(f'Failure on sending failure to task {taskid}...')
            logging.error(f'Error: {sys.exc_info()}')

    # New Lockduration

    def new_lockduration(self, taskid, new_duration):
        endpoint = str(self.url) + '/external-task/' + taskid + '/extendLock'

        request = {
            'workerId': self.workerid,
            'newDuration': new_duration
        }

        try:
            resp = requests.post(endpoint, json=request)
            self.validate_requests(resp)
            logging.info(
                f'Task {taskid} lock duration modified to {new_duration}...')
        except:
            logging.error(f'Failure on modifying lock to task {taskid}...')
            logging.error(f'Error: {sys.exc_info()}')

    def validate_requests(self, resp):
        if(resp.status_code not in(200, 204)):
            raise Exception(
                f'Status code {resp.status_code}. Error {resp.text}')


class process_client:
    def __init__(self, url):
        self.url = str(url)

    def correlate_message(self, message_name, business_key, **kwargs):
        endpoint = str(self.url) + '/message'

        variables_for_request = {}
        for key, val in kwargs.items():
            variables_for_request.update({key: val})

        request = {
            'messageName': message_name,
            'businessKey': business_key,
            'processVariables': variables_for_request
        }

        try:
            resp = requests.post(endpoint, json=request)
            self.validate_requests(resp)
            logging.info(
                f'Message {message_name} with business key {business_key} correlated.')
        except:
            logging.error(
                f'Failure on correlating message {message_name} with business key {business_key}')
            logging.error(f'Error: {sys.exc_info()}')


class camutil:

    @staticmethod
    def longval(val):
        return {'value': val, 'type': 'Long'}

    @staticmethod
    def doubleval(val):
        return {'value': val, 'type': 'Double'}

    @staticmethod
    def stringval(val):
        return {'value': val, 'type': 'String'}

    @staticmethod
    def boolval(val):
        return {'value': val, 'type': 'Boolean'}

    @staticmethod
    def get_business_key(task):
        return task.get('businessKey')

    @staticmethod
    def get_variable_val(task, varname):
        return task.get('variables').get(varname, {}).get('value', '')

    @staticmethod
    def get_variables(task):
        return task.get('variables')

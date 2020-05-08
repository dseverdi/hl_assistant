import pandas as pd

# user data
import HL_access

import json
from ibm_watson import AssistantV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator



class WatsonLogger:
    """
    logger for Watson assistant
    """
    def __init__(self,access):
        self.apikey, self.url, self.version, self.workspace_id = access.apikey, access.url, access.version, access.workspace_id

        authenticator = IAMAuthenticator(self.apikey)
        self.assistant = AssistantV1(
            version = self.version,
            authenticator = authenticator
        )
        self.assistant.set_service_url(self.url)

        self.response = self.assistant.list_logs(
            workspace_id=self.workspace_id
        ).get_result()

        


    def get_info(self):        
        response = self.assistant.list_workspaces().get_result()
        return json.dumps(response, indent=2)

    def save_logs(self):        
        with open('logs.json', 'w') as out:
            json.dump(self.response,out)

    def get_dataframe(self):
        """
        construct pandas DataFrame

        :returns: pandas.DataFrame  

        """
        ## stupci za df
        f_conversation_id = 'conversation_id'
        f_request_timestamp = 'request_timestamp'
        f_response_timestamp = 'response_timestamp'
        f_user_input = 'User Input'
        f_output = 'Output'
        f_intent = 'Intent'
        f_confidence = 'Confidence'
        f_exit_reason = 'Exit Reason'
        f_logging = 'Logging'
        f_context = 'Context'

        columns = [
            'conversation id','request time','response time', 'user input', 'output'
        ]
                   

        # retci DF-a
        rows = []
        
        for data_records in self.response['logs']:
            row = {}
            # get request
            row['conversation id']  = data_records['request']['context']['conversation_id']
            row['request time']     = data_records['request_timestamp']
            row['response time']    = data_records['response_timestamp']
            row['user input']       = data_records['request']['input']['text']
            row['output']           = " ".join(data_records['response']['output']['text'])

            # add to list
            rows.append(row)

        # Build the dataframe. 
        df = pd.DataFrame(rows,columns=columns)

        # cleaning up dataframe. Removing NaN and converting date fields. 
        df = df.fillna('')
        df['request time'] = pd.to_datetime(df['request time'])
        df['response time'] = pd.to_datetime(df['response time'])

        # Lastly sort by conversation ID, and then request, so that the logs become readable. 
        df = df.sort_values(['conversation id', 'request time'], ascending=[True, True])
      
        return df

if __name__ == '__main__':
    HL_logs = WatsonLogger(HL_access)

    # spremi logove u json
    HL_logs.get_dataframe()
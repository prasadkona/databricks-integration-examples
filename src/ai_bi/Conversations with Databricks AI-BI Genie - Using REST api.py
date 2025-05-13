# Databricks notebook source
# MAGIC %md
# MAGIC # Conversations with Databricks AI-BI Genie - Using REST api
# MAGIC
# MAGIC In this notebook we walk you through how to use the Genie conversation api to chat with your data.
# MAGIC
# MAGIC This example leverages the Databricks REST api's

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## What is an AI/BI Genie space
# MAGIC AI/BI Genie, allows business teams to interact with their data using natural language. It uses generative AI tailored to your organization’s terminology and data, with the ability to monitor and refine its performance through user feedback.
# MAGIC
# MAGIC ## Overview
# MAGIC Domain experts, such as data analysts, configure Genie spaces with datasets, sample queries, and text guidelines to help Genie translate business questions into analytical queries. After set up, business users can ask questions and generate visualizations to understand operational data. Genie’s semantic knowledge gets updated as your data changes and users pose new questions. ![](path)
# MAGIC
# MAGIC AI/BI Genie selects relevant names and descriptions from annotated tables and columns to convert natural language questions to an equivalent SQL query. Then, it responds with the generated query and results table, if possible. If Genie can’t generate an answer, it can ask follow-up questions to clarify before providing a response.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Learn more at
# MAGIC
# MAGIC * Documentation page https://docs.databricks.com/aws/en/genie/
# MAGIC * API docs (REST) : https://docs.databricks.com/api/workspace/genie
# MAGIC * API docs (Databricks Python SDK) : https://databricks-sdk-py.readthedocs.io/en/latest/workspace/dashboards/genie.html
# MAGIC

# COMMAND ----------

# DBTITLE 1,Calling Genie Conversation API
import requests
import time
import json


# Get the token stored using dbutils
token = dbutils.secrets.get(scope="<databricks_secrets_scope_name>", key="DATABRICKS_TOKEN")
host = "<databricks_workspace_host_name>"

def genie_conversation(space_id, initial_question, follow_up_questions=None, n_follow_ups=0, host=None, token=None):
    """
    Perform a conversation using Databricks Genie Conversation APIs.
    
    Args:
    space_id (str): ID of the Genie space
    initial_question (str): The initial question to ask
    follow_up_questions (list, optional): A list of follow-up questions
    n_follow_ups (int): Number of follow-up questions to ask
    host (str): Databricks workspace instance name
    token (str): Databricks authentication token
    
    Returns:
    dict: Conversation results including questions, answers, and SQL results
    """
    if not host or not token:
        raise ValueError("Host and token must be provided")

    headers = {'Authorization': f'Bearer {token}'}
    base_url = f"https://{host}/api/2.0/genie/spaces/{space_id}"

    def api_request(method, endpoint, data=None):
        url = f"{base_url}/{endpoint}"
        response = requests.request(method, url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()

    def wait_for_completion(conversation_id, message_id):
        while True:
            response = api_request('GET', f"conversations/{conversation_id}/messages/{message_id}")
            status = response.get('status')
            if status == 'COMPLETED':
                return response
            elif status in ['FAILED', 'CANCELLED', 'QUERY_RESULT_EXPIRED']:
                raise Exception(f"Message processing {status}")
            time.sleep(5)

    def get_query_result(conversation_id, message_id, attachment_id):
        while True:
            response = api_request('GET', f"conversations/{conversation_id}/messages/{message_id}/query-result/{attachment_id}")
            state = response['statement_response']['status']['state']
            if state == 'SUCCEEDED':
                return response['statement_response']['result']
            elif state in ['RUNNING', 'PENDING']:
                time.sleep(5)
            else:
                return None

    # Start conversation
    initial_response = api_request('POST', 'start-conversation', {'content': initial_question})
    conversation_id = initial_response['conversation_id']
    initial_message_id = initial_response['message_id']

    # Wait for initial question completion
    initial_message = wait_for_completion(conversation_id, initial_message_id)

    result = {
        'initial_question': initial_question,
        'initial_answer': initial_message.get('content'),
        'initial_sql_result': None,
        'initial_sql_text': None
    }

    # Get SQL result if available
    for attachment in initial_message.get('attachments', []):
        result['initial_sql_text'] = attachment['query']['query']
        result['initial_answer'] = attachment['query']['description']
        result['initial_sql_result'] = get_query_result(conversation_id, initial_message_id, attachment['attachment_id'])

    # Handle follow-up questions
    follow_up_results = []
    if follow_up_questions and n_follow_ups > 0:
        for i in range(min(n_follow_ups, len(follow_up_questions))):
            follow_up_question = follow_up_questions[i]
            follow_up_response = api_request('POST', f"conversations/{conversation_id}/messages", {'content': follow_up_question})
            follow_up_message_id = follow_up_response['message_id']

            follow_up_message = wait_for_completion(conversation_id, follow_up_message_id)

            follow_up_result = {
                'sequence_number': i + 1,
                'follow_up_question': follow_up_question,
                'follow_up_answer': follow_up_message.get('content'),
                'follow_up_sql_result': None,
                'follow_up_sql_text': None
            }

            # Get SQL result for follow-up if available
            for attachment in follow_up_message.get('attachments', []):
                follow_up_result['follow_up_sql_text'] = attachment['query']['query']
                follow_up_result['follow_up_answer'] = attachment['query']['description']
                follow_up_result['follow_up_sql_result'] = get_query_result(conversation_id, follow_up_message_id, attachment['attachment_id'])

            follow_up_results.append(follow_up_result)

    result['follow_up_results'] = follow_up_results
    return result



# COMMAND ----------

# Example usage:
result = genie_conversation(
     space_id="01f00ab3b6441e93b89ebeb1bbd3d00f",
     initial_question="Which industries have shown the highest engagement rates with marketing campaigns?",
     follow_up_questions=["how many prospects for construction?", "what are the top three industries?"],
     n_follow_ups=2,
     host=host,
     token=token
)
print(json.dumps(result, indent=2))
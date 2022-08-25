import json
import adal
import requests
import os
import azure.mgmt.resource
import automationassets
import pandas as pd
from pretty_html_table import build_table
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date, timedelta

gmail_user = 'rzariwal'
gmail_password = 'zlvzybanthsaqwsn'


class AzureUsage:

    ######################### Getting the login OAuth Token #################################################

    def __init__(self):
        runas_connection = automationassets.get_automation_connection(
            "AzureRunAsConnection")
        tenant_id = runas_connection["TenantIdPersonal"]
        subscription_id = runas_connection["SubscriptionIdPersonal"]
        scope = "subscriptions/" + subscription_id
        self.costmanagementUrl = "https://management.azure.com/" + scope + \
                                 "/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
        authority_uri = automationassets.get_automation_variable("activeDirectoryEndpointUrl") + \
            "/" + tenant_id
        context = adal.AuthenticationContext(authority_uri)
        token = context.acquire_token_with_client_credentials(
            automationassets.get_automation_variable(
                "resourceManagerEndpointUrl"),
            automationassets.get_automation_variable("clientIdPersonal"),
            automationassets.get_automation_variable("clientSecretPersonal"))
        bearer = "bearer " + token.get("accessToken")

        self.headers = {"Authorization": bearer,
                        "Content-Type": "application/json"}
        self.usagedata = []

    ########################## Getting the Usage data of your resources ###########################################################

    def run(self, startdate, enddate, grain="Daily", groupby=None):

        payload = {
            "type": "ActualCost",
            "dataSet": {
                "granularity": grain,
                "aggregation": {
                    "totalCost": {
                        "name": "PreTaxCost",
                        "function": "Sum"
                    },
                    "totalCostUSD": {
                        "name": "PreTaxCostUSD",
                        "function": "Sum"
                    }
                }
            },
            "timeframe": "Custom",
            "timePeriod": {
                "from": startdate,
                "to": enddate
            }
        }

        if groupby != None:
            payload['dataSet']['grouping'] = [{
                "type": "Dimension",
                "name": groupby
            }]

        payloadjson = json.dumps(payload)
        print("Payload Json: ", payloadjson)
        self.usagedata = []
        response = requests.post(
            self.costmanagementUrl, data=payloadjson, headers=self.headers)
        if response.status_code == 200:
            self.transform(payloadjson, response.text)
        else:
            print("error")
            print("error " + response.text)

        return self.usagedata

    ######################## Tranforming the response based on cost properties #####################################

    def transform(self, payloadjson, response):
        result = json.loads(response)
        print("Result: ", result)
        for record in result["properties"]["rows"]:
            usageRecord = {}
            for index, val in enumerate(record):
                columnName = result["properties"]["columns"][index]
                if columnName["type"] == "Number":
                    usageRecord[columnName["name"]] = val
                else:
                    usageRecord[columnName["name"]] = val

            self.usagedata.append(usageRecord)

        nextLink = result["properties"]["nextLink"]
        if nextLink != None:
            nextLinkResponse = requests.post(
                nextLink, data=payloadjson, headers=self.headers)
            if nextLinkResponse.status_code == 200:
                self.transform(payloadjson, nextLinkResponse.text)
            else:
                print("error in fetching next page " + nextLink)
                print("error " + nextLinkResponse.text)

    ############################ Making the dataframe based on Resource group ###############################

    def makeResourceWiseTable(self, usageData):
        data = usageData
        df = pd.DataFrame(data)
        df.sort_values('ResourceGroup', inplace=True)
        df.drop_duplicates(subset='ResourceGroup', keep='first', inplace=True)

        return df

    ################################## Making the dataframe based on Total Cost ###############################################

    def makeTotalCostTable(self, resourceWiseCostData):
        resourceCostData = resourceWiseCostData
        column_sum = resourceCostData['PreTaxCost'].sum()

        cost = []
        totalCost = [column_sum]

        for item in totalCost:
            cost.append(item)

        df = pd.DataFrame(cost, columns=['TotalCost'])

        return df

    ################################### Sending mail of Daily Usage Report ##################################################

    def send_mail(self, body1, body2):
        message = MIMEMultipart()
        sender = automationassets.get_automation_variable("FromMail")
        receivers = ['rzariwal@gmail.com']
        # receivers = 'xxx.mycompany.com'
        message['Subject'] = 'Daily Cost Analysis Report'
        message['From'] = sender
        message['To'] = ";".join(receivers)
        # message['To'] = sender
        body_content = "<h3>Date: <font color=#33cc33>"+convertToStrpday+" <font color=#000000>to ""<font color=#33cc33>"+convertToStrtday + \
            "</h3><h3><font color=#000000>Subscription: <font color=#0099ff><SubscriptionNamw></h3><h2><font color=#000000>Total Cost of Single Day Usage:</h2>" + \
            body2+"\n"+"<h2><font color=#000000>Cost of Individual Resource Group:</h2>"+body1

        message.attach(MIMEText(body_content, "html"))
        msg_body = message.as_string()

        smtpObj = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        smtpObj.login(gmail_user, gmail_password)
        smtpObj.sendmail(sender, receivers, msg_body)

    ######################################## Making the attaractive table ############################################

    def send_report(self, usageData):
        tabulate_form1 = self.makeResourceWiseTable(usageData)
        tabulate_form2 = self.makeTotalCostTable(tabulate_form1)
        output1 = build_table(tabulate_form1, 'yellow_dark')
        output2 = build_table(tabulate_form2, 'green_dark')
        try:
            self.send_mail(output1, output2)
            print('Mail sent successfullly!!')
        except Exception as error:
            print(error)
            print('Unable to send Mail')


def run_script():
    azure_usage = AzureUsage()
    # usageResult = azure_usage.run("2021-09-27", "2021-09-28")

    usageResult = azure_usage.run(
        "2022-08-13", "2022-08-14", groupby="ResourceGroup")
    print(*usageResult, sep="\n")
    azure_usage.send_report(usageResult)
    print("Done")


################################## Getting the Yesterday and Today's date ####################################

today = date.today()
previuosDate = today-timedelta(days=1)
convertToStrtday = "2022-08-14"
convertToStrpday = "2022-08-13"


################## Main function ########################

if __name__ == "__main__":
    run_script()

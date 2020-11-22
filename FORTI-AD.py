import requests
import json
import urllib3
import pandas as pd
import time
import math
import subprocess
import sys
import os


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # To supress certificate # WARNING:


#From active directory we pull user information with the specific attribute(extensionAttribute7) --> Company Name of the User
#                                                                 mail --> EMail address of  the User
#                                                                 extensionAttribute10 -->   Mobile_number of  the User


subprocess.run('dsquery  * CN=Users,DC=__input__,DC=__input__,dc=__input__ -filter "(&(objectClass=user)(extensionAttribute7=__input__))" -limit 0 -attr mail extensionAttribute10 > __Output_Directory__/Users.csv',shell=True)

# Time sleep if AD query takes so much time
time.sleep(20)

#Read AD query output with Pandas DataFrame
Users_DataFrame = pd.read_csv("__Output_Directory__/Users.csv")

# Formatted_UserList_DataFrame is used to format ipdf dataframe for further usage. Formatted_Users_DataFrame has 2 columns which is "Email and Mobile Number"
AD_Users_DataFrame = pd.DataFrame(columns = ["Email","Tel"])

# Report Users who has no mobile number in active directory and write to the No_Tel text file
file = open('__No_Mobil_Tel_OUTPUT_DIRECTORY__\No_TEL.txt', 'w+')

# Format AD Query output and identify users who have no mobile number in active directory
User_List = Users_DataFrame.values.tolist()

for element in User_List:
    element = ''.join(element)
    element = element.split()

    if(len(element) == 2):
        AD_Users_DataFrame = AD_Users_DataFrame.append({'Email':element[0],'Tel':element[1]},ignore_index=True)

    else:
        file.write(element[0])  # Users who has no mobile tel

file.close()


# We need to determine the "TOTAL NUMBER OF USERS" in the "FORTI AUTHENTICATOR"

payload = ""
headers = {"Content-type": "application/json",
          }
User_Count_Url = "__AUTHENTICATOR_IP__/api/v1/ldapusers/?limit=1"

User_Count_Response = requests.request("GET",User_Count_Url,auth=('__API_USER__','__API_KEY__'),headers= headers,verify=False).json()
Total_User_Count=User_Count_Response['meta']['total_count']

Quotient = math.floor(Total_User_Count / 1000)
Remainder  = Total_User_Count % 1000


# GETTING USERS FROM FORTI AUTHENTICATOR WITH THE PARAMETERS OF EMAIL AND USERS UNIQUE ID
# WE USED TOTAL NUMBER OF USERS VALUES IN HERE BECAUSE API PAYLOAD LIMIT IS 1000

email_list = []   #  This empty list is used for User Email
id_list =[]          #  This empty list is used for User ID

for x in range(0,Quotient):

    Get_User_Url = "__AUTHENTICATOR_IP__/api/v1/ldapusers/?limit=1000&offset={}".format(x*1000)
    User_Response = requests.request("GET",Get_User_Url,auth=('__API_USER__','__API_KEY__'),headers= headers,verify=False)

    for i in range(0,1000):

        id_list.append(User_Response.json()['objects'][i]['id'])
        email_list.append(User_Response.json()['objects'][i]['email'])


Get_User_Url = "__AUTHENTICATOR_IP__/api/v1/ldapusers/?limit=1000&offset={}".format(Quotient*1000)
User_Response = requests.request("GET",Get_User_Url,auth=('__API_USER__','__API_KEY__'),headers= headers,verify=False)

for k in range(0,Remainder):
    id_list.append(User_Response.json()['objects'][k]['id'])
    email_list.append(User_Response.json()['objects'][k]['email'])

#User informations from forti auth is formatted here by concatanating EMAIL AND UNIQUE ID

email_series = pd.Series(email_list) # Convert username_list to Username Series
idlist_series = pd.Series(id_list) # Convert id_list list to idlist Series

Forti_Users_DataFrame = pd.concat([email_series,idlist_series],axis=1) # Crate dataframe which contains email and unique ID
Forti_Users_DataFrame.columns = ['Email','ID']
Forti_Users_DataFrame['Email'] = Forti_Users_DataFrame['Email'].str.upper()

#Merge dataframes from Forti Auth and AD output with the EMAIL column and resulting dataframe consists "Username(EMAIL),Unique ID,mobile_number"
Merged_DataFrame = pd.merge(Forti_Users_DataFrame, AD_Users_DataFrame, on="Email")


#This dataframe is used for comparing old user information and new information
#This is why we called that variable reference variable
#Reference output dataframe is a one time output of Merged_DataFrame
#Reference DataFrame is static and never changed over time. But Merged_DataFrame can be changed when the code is executed.
Reference_DataFrame = pd.DataFrame(columns = ['Email','ID','Tel'])
Reference_DataFrame = pd.read_csv("__REFERENCE_OUTPUT_DIRECTORY__\Reference.csv")


#Compare new merged dataframe with the reference dataframe to identify changes and create new dataframe which contains users who information is changed
Changed_Users_DataFrame = pd.DataFrame(columns = ['Email','ID','Tel'])
Changed_Users_DataFrame= pd.concat([Reference_DataFrame,Merged_DataFrame]).drop_duplicates(keep=False)
Changed_Users_DataFrame.drop_duplicates(subset ="Email", keep = 'last', inplace = True)


#We change the user mobile_number if it is changed in the active directory
for user in Changed_Users_DataFrame.values:

    Unique_User_Url = "__AUTHENTICATOR_IP__/api/v1/ldapusers/{}/".format(user[1])
    Unique_User_Response = requests.request("PATCH",Unique_User_Url,auth=('__API_USER__','__API_KEY__'),headers= headers,verify=False,json={"mobile_number":"{}".format("+"+str(user[2]))})

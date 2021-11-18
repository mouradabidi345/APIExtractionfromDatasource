#InisightAPI before automation the time
import Levenshtein
import pandas as pd
import xlrd
import sqlalchemy
import math
import numpy as np
import datetime
import pandas as pd
import xlrd
import sqlalchemy
import math
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from io import StringIO
import http.client
import json
import xlrd
import xlsxwriter
import xlwings as xw
import openpyxl as pxl


#get the Agent base which Holds the Agents IncontctIDs and Email addresses:
engine = sqlalchemy.create_engine("mssql+pyodbc://mabidi:ChangeMe2020$@ReportsWFM")
Agents = pd.read_sql_query('SELECT * FROM ASEA_REPORTS_STAGE.dbo.[Agent_Base]', engine)
IncontactIDLis = Agents['InContactID'].tolist()

#get the list of Contributed articles Emailed to WFM so we can match it with the list of Contributed articles coming from Raw data itself, in case management wants 
# to not consider some of those of Contributed articles:
Emailed = pd.read_excel(r"Z:\Shared\Associate Support\WFM/Legit_Insight_Articles.xlsx", sheet_name = 0, engine='openpyxl')
#print(Emailed)
#Emailed = Emailed.dropna(how='any')
#print(Emailed)
d= datetime.datetime.today()
sun_offset = (d.weekday() - 6) % 7
sunday_same_week = d - datetime.timedelta(days=sun_offset)
td = datetime.timedelta(days=7)
Report_Start_Date = (sunday_same_week - td).strftime("%Y-%m-%d")
#filter the the list of Contributed articles Emailed to WFM by the Report_Start_Date
EmailedFilteredbyWeekStartSDate = Emailed[Emailed['WeeK_Start_Date'] == Report_Start_Date]
print(EmailedFilteredbyWeekStartSDate)
Posted = len(EmailedFilteredbyWeekStartSDate.index)
#print(Posted)
#print(Report_Start_Date)
S = [Report_Start_Date]
P = [Posted]
NewArticles = pd.DataFrame()
NewArticles['Report_Start_Date'] = S
NewArticles['Posted'] = P
print(NewArticles)
engine = sqlalchemy.create_engine("mssql+pyodbc://mabidi:ChangeMe2020$@ReportsWFM")
numRows = len(NewArticles.index)
numCols = len(NewArticles.columns)
tableName = "Contributed"
print("Inserting " + str(numRows) + " rows into " + tableName)
chunksize = math.floor(2100 / numCols) - 1
NewArticles.to_sql(tableName, engine, 
            if_exists = 'append', index = False, 
            chunksize = chunksize, method = 'multi')







#get the raw data  from Bloomfire using APIs:
d= datetime.datetime.today()
Mon_offset = (d.weekday() - 0) % 7
Monday_same_week = d - datetime.timedelta(days=Mon_offset)
Monday_same_weeks = Monday_same_week.strftime("%Y-%m-%d")
td = datetime.timedelta(days=7)
Monday_previous_week = (Monday_same_week - td).strftime("%Y-%m-%d")
Urlendpoint = "https://reports-api.bloomfire.com/" + "member_engagement/full.csv?" + "date_range=" + Monday_previous_week + "%20to%20" + Monday_same_weeks





#open http connection to client bloomfire
conn = http.client.HTTPSConnection("asea-global.bloomfire.com")

# convert payload to json
payload = json.dumps({
  "api_key": "0d2168dd2079e8ef1e4e63fc13e8b53d801840ca",
  "email": "ITdev@aseaglobal.com"
})

#Set headers
headers =  {
                'Content-Type': 'application/json',
                'Bloomfire-Requested-Fields': 'reports_api_token'
            }
#request connection to api
conn.request("POST", "/api/v2/login", payload, headers)
#read response
res = conn.getresponse()
data = res.read()
#convert from bytes to string
data = data.decode("utf-8")
data = json.loads(data)
#close connections that were opened when getting token
res.close()
conn.close()

#assign token to bloomfire_token variable
bloomfire_token = data["reports_api_token"]["token"]

#open another http connection to client bloomfire
conn1 = http.client.HTTPSConnection("reports-api.bloomfire.com")
payload1 = ''
headers1 = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer' + ' ' + bloomfire_token
            }

#request connection to api
conn1.request("GET", Urlendpoint, payload1, headers1)

#read response
res1 = conn1.getresponse()
data = res1.read()
#conevrt data from bytes to string
data = data.decode("utf-8")
res.close()
conn.close()
#insert the string data into a dataframe
StringData = StringIO(data)
df = pd.read_csv(StringData, sep =",")
#print(df)
# print(type(df['Date']))

#oldapi
# Urlendpoint = "analytics/" + "member_engagement:full.csv?" + "date_range=" +  Monday_previous_week + "%20to%20" + Monday_same_weeks

#            #get teh sessiontoken
# conn = http.client.HTTPSConnection("asea-global.bloomfire.com")
# payload = "{\r\n  \"email\": \"Mabidi@aseaglobal.com\",\r\n  \"password\": \"Ma@fawzia15\"\r\n}"
# headers = {
#   'Content-Type': 'application/json'
# }
# conn.request("POST", "/api/v2/login", payload, headers)
# res = conn.getresponse()
# data = res.read()
# data = json.loads(data)
# res.close()
# conn.close()
# session = data["session_token"]

#            #Api to get the raw data
# conn = http.client.HTTPSConnection("asea-global.bloomfire.com")
# payload = ''
# headers = {
#   'Authorization': 'Bloomfire-Session-Token' +  session
# }
# conn.request("GET", "/api/v2/" + Urlendpoint, payload, headers)
# res = conn.getresponse()
# data = res.read()
#               #conevrt raw data from bytes to string using .decode("utf-8")
# data = data.decode("utf-8")
# res.close()
# conn.close()
#            #insert the string raw data into a dataframe using StringIO()
# StringData = StringIO(data)
# df = pd.read_csv(StringData, sep =",")
# dfcount = len(df.index)
# print(dfcount)
          #convert the Date column back from String to Datetime type
df['Date']= pd.to_datetime(df['Date'])
          #add a new column Date2 which has the format yyyy-m-d
df['Date2'] = df['Date'].dt.strftime("%Y-%m-%d")
          #remove duplicate rows
df = df.drop_duplicates(subset = ['Email','Date2','Content Type','Title','Action'], keep = 'first')
dfcount = len(df.index)
print(dfcount)
#join the Agent base with the data coming from The Api on the Email column key
dfmerged = pd.merge(Agents,df,on='Email', how='right')
#dfmergedcount = len(dfmerged.index)
#print(dfmergedcount)
#filter the result when the Titles are Contributed
df1 = dfmerged.query('Action == "Contribute"')
df2 = df1.Title
#print(df2)
List1 = df2.tolist() 

#Correct the spelling of the titles of the Contributed articles which were sent by the managers to WFM if it is incorrect:
actual_title = []
similarity = []
for i in EmailedFilteredbyWeekStartSDate.Title:
    ratio = process.extract( i, dfmerged.Title, limit=1)
    actual_title.append(ratio[0][0])
    similarity.append(ratio[0][1])
EmailedFilteredbyWeekStartSDate.loc[:,'actual_title'] = actual_title
EmailedFilteredbyWeekStartSDate.loc[:,'similarity'] = similarity
print(EmailedFilteredbyWeekStartSDate)
TitlesEmailed = EmailedFilteredbyWeekStartSDate['actual_title']
lists = TitlesEmailed.tolist()
size = len(lists)

# actual_title = []
# similarity = []
# for i in EmailedFilteredbyWeekStartSDate.Title:
#     ratio = process.extract( i, df1.Title, limit=1)
#     actual_title.append(ratio[0][0])
#     similarity.append(ratio[0][1])
# EmailedFilteredbyWeekStartSDate.loc[:,'actual_title'] = actual_title
# EmailedFilteredbyWeekStartSDate.loc[:,'similarity'] = similarity
# print(EmailedFilteredbyWeekStartSDate)
# TitlesEmailed = EmailedFilteredbyWeekStartSDate['actual_title']
# lists = TitlesEmailed.tolist()
# size = len(lists)



#use only the list of titles of the contributed articles which were  emailed to WFM by the managers, 
# if no titles were emailed then use the Contributed Articles's titles of the dataframe coming from the API:
if size == 0:
    Newsfeed = df1['Title'].tolist()
else:
    Newsfeed = lists

length = len(Newsfeed )

#create an empty dataframe to load the end result into it, and insert the column headers into it:
dfnew = pd.DataFrame() 
dfnew ['InContactID'] = IncontactIDLis
dfnew.insert(1, "Sum", "")
dfnew.set_index('InContactID', inplace=True)

for i in range(length):
    dfnew.insert(1, Newsfeed[i], "")
dfnew

readCount = 0
for j in IncontactIDLis:
    readCount = 0
    for i in Newsfeed:
        dfnew.loc[j, i] = len(dfmerged[(dfmerged.Title==i) & (dfmerged.Action=='View') & (dfmerged.InContactID==j)].index) #load the count of each article read by each agent
        count = len(dfmerged[(dfmerged.Title==i) & (dfmerged.Action=='View') & (dfmerged.InContactID==j)].index)
        if (count >= 1):
            readCount += 1
    dfnew.loc[j, 'Sum'] = readCount  #insert the sum of the count of each article read by each agent

#add a Report Start date column so we can use it as a filter in Power BI   
d= datetime.datetime.today()
sun_offset = (d.weekday() - 6) % 7
sunday_same_week = d - datetime.timedelta(days=sun_offset)
td = datetime.timedelta(days=7)
Report_Start_Date = (sunday_same_week - td).strftime("%Y-%m-%d")
dfnew.insert(0,"Report_Start_Date",Report_Start_Date)
dfnew = dfnew.reset_index()
#drop any row which has any NA value
dfnew = dfnew.dropna(how='any')
print(dfnew)
#Only select the columns 'InContactID','Report_Start_Date','Sum
dfnew = dfnew[['InContactID','Report_Start_Date','Sum']]
print(dfnew)
engine = sqlalchemy.create_engine("mssql+pyodbc://mabidi:ChangeMe2020$@ReportsWFM")
numRows = len(dfnew.index)
numCols = len(dfnew.columns)
tableName = "Read_Articles"
print("Inserting " + str(numRows) + " rows into " + tableName)
chunksize = math.floor(2100 / numCols) - 1
dfnew.to_sql(tableName, engine, 
            if_exists = 'append', index = False, 
            chunksize = chunksize, method = 'multi')




Workedday = pd.DataFrame()
Workedday ['InContactID'] = IncontactIDLis
Workedday.insert(1, "Workeddays", "")


print(Workedday)
Workedday = Workedday.set_index('InContactID')
print(Workedday)
for j in IncontactIDLis:
    Workedday.loc[j, 'Workeddays'] = len(dfmerged[(dfmerged.Action=='Login Success') & (dfmerged.InContactID==j)].index)


#Workedday = Workedday.set_index('InContactID')
Report_Start_Date = (sunday_same_week - td).strftime("%Y-%m-%d")
Workedday.insert(1,"Report_Start_Date",Report_Start_Date)
Workedday = Workedday.reset_index()
print(Workedday)
engine = sqlalchemy.create_engine("mssql+pyodbc://mabidi:ChangeMe2020$@ReportsWFM")
numRows = len(Workedday.index)
numCols = len(Workedday.columns)
tableName = "Workeddays"
print("Inserting " + str(numRows) + " rows into " + tableName)
chunksize = math.floor(2100 / numCols) - 1
Workedday.to_sql(tableName, engine, 
            if_exists = 'append', index = False, 
            chunksize = chunksize, method = 'multi')







ViewedArticles = pd.DataFrame()
ViewedArticles ['InContactID'] = IncontactIDLis


ViewedArticles.insert(1, "TotalViewed", "")
print(ViewedArticles)
ViewedArticles = ViewedArticles.set_index('InContactID')
print(ViewedArticles)
for j in IncontactIDLis:
    ViewedArticles.loc[j, 'TotalViewed'] = len(dfmerged[(dfmerged.Action=='View') & (dfmerged.InContactID==j)].index)

#ViewedArticles = ViewedArticles.reset_index('InContactID')

Report_Start_Date = (sunday_same_week - td).strftime("%Y-%m-%d")
ViewedArticles.insert(1,"Report_Start_Date",Report_Start_Date)
ViewedArticles = ViewedArticles.reset_index()
print(ViewedArticles)
engine = sqlalchemy.create_engine("mssql+pyodbc://mabidi:ChangeMe2020$@ReportsWFM")
numRows = len(ViewedArticles.index)
numCols = len(ViewedArticles.columns)
tableName = "Viewed"
print("Inserting " + str(numRows) + " rows into " + tableName)
chunksize = math.floor(2100 / numCols) - 1
ViewedArticles.to_sql(tableName, engine, 
            if_exists = 'append', index = False, 
            chunksize = chunksize, method = 'multi')




engine = sqlalchemy.create_engine("mssql+pyodbc://mabidi:ChangeMe2020$@ReportsWFM")
df1 = dataframe = pd.read_excel(r"C:\Users\mabidi\ASEA, LLC\Associate Support Operations - Insight/Insight Newsfeed Content.xlsx", sheet_name = 0, engine='openpyxl')
engine = sqlalchemy.create_engine("mssql+pyodbc://mabidi:ChangeMe2020$@ReportsWFM")
df0 = pd.read_sql_query('SELECT * FROM ASEA_REPORTS_STAGE.dbo.[Titles Emailed]', engine)
df0.drop(df0.index, inplace=True)
df0 = df0.append(df1)
print(df0)
numRows = len(df0.index)
numCols = len(df0.columns)
tableName = "Titles Emailed"
print("Inserting " + str(numRows) + " rows into " + tableName)
chunksize = math.floor(2100 / numCols) - 1
df0.to_sql(tableName, engine, 
            if_exists = 'replace', index = False, 
            chunksize = chunksize, method = 'multi')

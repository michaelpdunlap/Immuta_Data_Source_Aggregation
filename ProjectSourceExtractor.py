#################################
#                               #
#       Project Source          #
#          Extractor            #
#                               #
# By: Mike Dunlap, Immuta CSA   #
# Born On: 8/27/2021            # 
#                               #
#################################

# Purpose
# Provide a snapshot of schema project connections and the connections
# of the underlying data source
#
# Basic instruction
# Run the script - export the data to the file of your choice

#Install json
import json

#Import requests
import requests

#Import Pandas
import pandas

# Immuta Instance variables
# Set the hostname (and port if not 443) for the Immuta instance
IMMUTA_URL= "https://operationdatabrickhouse1.internal.immuta.com"
# This is your user API key from Immuta
API_KEY= "e7b7f52d73e14472a0d80894b8d9ac55"

# Databricks source type
sourceTypeSearch = "443" 

# get your authentication token
response = requests.post(
  IMMUTA_URL + '/bim/apikey/authenticate',
  headers={'Content-Type': 'application/json'},
  json={
    "apikey": API_KEY
  }
)

# get the auth token out of the json response
authResponse = response.json()
authToken = authResponse["token"]
#print(authToken)

# now let's try to get all connections for a project
  
projectResponse = requests.get(
  IMMUTA_URL + '/project',
  headers={'Authorization': authToken }  
)
  
# projectResponse.json()
  
# print (projectResponse.json())

# start lists to form pandas df

projectNameList = []
projectConnectionList = []
dataSourceIDList = []
dataSourceNameList = []
dataSourceConnectionList = []
  
# swell, now let's loop through the results
  
projects = projectResponse.json()
projectIDs = projects['hits']

for i in projectIDs:
  projectID = i['id']
  # projectDescription = i['description']
  # detect databricks schema projects
  if i['description'] == None:
    print (str(i['id']) + " is not a schema project.")
  else
    if sourceTypeSearch in i['description']: 
      # for each source type schema project, get the connections for the underlying data sources
      # call the get project data sources end point
      
      projectName = i['name']
      projectConnection = i['description']
      
      projectDataSources = request.get(
        IMMUTA_URL + "/" + str(i['id']) + "/dataSources",
        headers={'Authorization': authToken }
      )
      
      dataSources = projectDataSources.json()
      dataSourcesIDs = dataSources['hits']
      # get the info for each data source, and append relevant project info
      
      for z in dataSourceIDs:
        # from here, need to validate strongly due to inavailability of resources
        dataSourceID = z['id']
        dataSourceName = z['name']
        dataSourceConnection = z['connectionString']

        # gotta figure out here how to add to owners, there is a call for that
    
        # at this point, start appending to lists
        projectNameList.append(projectName)
        projectConnectionList.append(projectConnection)
        dataSourceIDList.append(dataSourceID)
        dataSourceNameList.append(dataSourceName)
        dataSourceConnectionList.append(dataSourceConnection)

# build and print final data frame
dataSourceSummary = pandas.DataFrame({'ProjectName':projectNameList},{'ProjectConnection':projectConnectionList},
{'DataSourceID':dataSourceIDList}, {'DataSourceName':dataSourceNameList}, {'DataSourceConnection':dataSourceConnectionList} )

print(dataSourceSummary)

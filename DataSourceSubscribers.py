
#################################
#                               #
#      DataSource Subscriber    #
#       Summary Population.     #
#                               #
# By: Mike Dunlap, Immuta CSA   #
# Born On: 8/4/2021             # 
#                               #
#################################

# Purpose
# Provide a summary dataframe describing the number of subscribers per data source
# Basic instruction
# Insert the Immuta URL & API key
# Dataobject can be packaged or 'swizzled' according to application



# Immuta Instance variables
# Set the hostname (and port if not 443) for the Immuta instance
IMMUTA_URL= "https://myimmuta.com"
# This is your user API key from Immuta
API_KEY= "1234luggagecombo4567"

# import libraries
import requests
import pandas
import json

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
# print(authToken)

# pull all source connections

sourceResponse = requests.get(
  IMMUTA_URL + '/dataSource',
  headers={'Authorization': authToken }  
)
  
sourceResponse.json()
# print (sourceResponse.json())
  
# swell, now let's loop through the results
  
sources = sourceResponse.json()

# print(sources)

sourceIDs = sources['hits']

# create the list variables
rowNameList = []
rowIDList = []
rowSubsList = []
lastUpdateList = []
createList = []

# goal here would be to grep the connection string into the description
for i in sourceIDs:
  sourceConnection = i['connectionString']
  # now let's get the number of subscribers per source
  response = requests.get(
    IMMUTA_URL + '/dataSource/' + str(i['id']) + '/access',
    headers={'Authorization': authToken,
            'Content-Type': 'application/json'}
  )
  getASource = response.json()
  # now let's get last updated
  response = requests.get(
    IMMUTA_URL + '/dataSource/' + str(i['id']) + '/activities',
    headers={'Authorization': authToken,
            'Content-Type': 'application/json'}
  )
  getActivity = response.json()
  # capture the data in lists
  rowName = i['name']
  rowID = i['id']
  rowSubs = getASource['count']
  for i in getActivity['activities']:
    rowUpdate = i['updatedAt']
  #rowUpdate = json_extract(getActivity.json(), 'updatedAt')
  createdAt = i['createdAt']
  rowNameList.append(rowName)
  rowIDList.append(rowID)
  rowSubsList.append(rowSubs)
  lastUpdateList.append(rowUpdate)
  createList.append(createdAt)
        
# create a data frame to capture source description, ID & number of subscribers from the list captured above
dataSource = pandas.DataFrame({'SourceID':rowIDList, 'SourceDesc':rowNameList, 'Subscribers':rowSubsList, 'Created': createList, 'LastUpdate':lastUpdateList})

print (dataSource)



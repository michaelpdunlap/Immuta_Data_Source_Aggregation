## Immuta Data Source Aggregation
The purpose of this repository is to capture various ways of manipulating Immuta data sources using the API and Python.

### ProjectSourceExtractor.py

This script will extract schema project and data source connections.  It will return a dataframe that you can export to your file system.

It currently relies upon entering a search string to narrow down the results returned.  It's defaulted to 443, the Databricks port.

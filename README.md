# Python workflow

This script was initially created with Jupyter.

Libraries used are:
- psycopg2 (for PostreSQL)
- numpy (for array processing to increase speed)
- pandas (for dataframe datastructure for our analysis)
- csv (to create csv files)

# Description

#### Context:

The client needed to synchronise their own application (`App A`) with a 3rd-party application `App B`.

#### Problem:

- the 3rd-party app can only sync data via integration or upload files
- Client is not able to provide data in the correct format
- 3rd-party app is not able to automate the transformation of files provided by the client
- Manual transformation is very time consuming (6-8 hours per week)

#### Solution:

This script was created to automate the creation of upload files to feed the 3rd-party app to ensure data were in sync.

This reduced the time to approximately 30 minutes per week (instead of 6-8 hours per week).

# Prerequites

This script was initially created with Jupyter to ease the analysis and transformation process.

To install:
- Postgres Server
- Packages (listed above)
- Python 3.6

# Highlights of the project

This project was a great opportunity to handle data in a real world scenario.

It was also interesting to observe how difficult this was for other users to adopt this new workflow in Python.

# Difficulties/barriers

The main difficulty was the fact that the script required the user to be familiar with Python.
Also the user needed to have a Python environment to run the script and it was a real barrier in terms of adoption.

Users are familiar with tools like Excel and it is not realistic to assume that those users would be able to run a Python script without prior knowledge of Python.

Therefore, to improve this project, we would either need to:
- provide the Python script via a clean user-interface. Porting this to the web would give more flexibility in terms of data and user management.
- or provide the Python script via an API that can be accessed directly via Excel.

# To-do

- Provide SQL tables
- Clean code (manage functions in separate files, use decorators)
- Ensure code is working with Python 3.8
- Create unit testing
- Provide script via AWS Lambda Functions
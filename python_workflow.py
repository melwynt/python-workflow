from platform import python_version
import psycopg2.extras
import psycopg2
import numpy as np
import pandas as pd
import csv
from datetime import datetime
datetime_1 = datetime.now()
print(str(datetime_1))

# List of files to import

WORK_MAIN = 'work/'
WORK_FOLDER = 'work/2021-05-24/'
WORK_OUTPUT = 'work/2021-05-24/output/'

# Notes
# ---
# Make sure that the report only fetches `open` users otherwise the last file that is created in this script can select the incorrect email address.

# This is the file we receive on a weekly basis
FILENAME_EBUY = WORK_FOLDER + "Carto-eBuy controlling.csv"

# This file is used to update the users table with emails
# We would need this file to be uploaded once
# And have the data retrieved with a select query
FILENAME_EFFECTIF = WORK_MAIN + "effectif.csv"

# used for variable df_users_FG_report
FILENAME_DELEGATES = WORK_FOLDER + "Delegates_basic.csv"

# All cost centres
FILENAME_CC_ALL = WORK_FOLDER + "new reports/Cost_Centres___ALL.csv"

# Cost_Centres_with_usernames
FILENAME_CC_Usernames = WORK_FOLDER + \
    "new reports/Cost_Centres_with_usernames.csv"

# All cost centres
FILENAME_USER_INFO = WORK_FOLDER + "new reports/User_information__to_copy_for_.csv"


def connect():
    """
    Connect to database

    Args:
        Function takes no arguments

    Returns:
        Function returns a connection

    Raises:
        Prints out an error message if it is not able to connect to database
    """
    try:
        connection = psycopg2.connect(dbname="work",
                                      user="melou",
                                      password="",
                                      host="localhost")
        print("Connected")
        return connection
    except:
        print("I am unable to connect to the database")


def run_query_select(qry):
    """
    Runs a select query

    Args:
        str: the SQL query

    Returns:
        result
    """
    with connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(qry)

            return cur.fetchall()


def run_query(qry):
    """
    Runs a query

    Args:
        str: the SQL query
    """
    with connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(qry)

# Cost Centres
# ---

# Create table for cost centres.<br>
# This won't do anything since the table is already there.


qry_create_table_for_cost_centres = """
        create table if not exists cost_centres(
        cost_centre_code text primary key,
        name text
        );
        """

run_query(qry_create_table_for_cost_centres)

# Here we're reading the csv file in `FILENAME_EBUY`.

# read the csv file and assign it to a variable
df_cost_centres = pd.read_csv(FILENAME_EBUY)

df_cost_centres.head()


def apply_leading_zeros(cc):
    """
    Applies leading zeros only to integers.

    Args:
        str: the cost centre code
    Returns:
        str: the cost centre code with leading zeros
             or the original code
    """
    if cc.isnumeric():
        return cc.zfill(10)
    else:
        return cc


df_cost_centres['new_cc_code'] = df_cost_centres['Cost center'].apply(
    lambda row: apply_leading_zeros(row))

df_cost_centres.head()

# delete previous column containing wrong format
del df_cost_centres['Cost center']

# reorder columns (https://stackoverflow.com/a/55204319/7246315)
df_cost_centres = df_cost_centres[[
    'new_cc_code', 'Désignation CC', 'Entity', 'Controller Name', 'CFO Name']]

# rename columns
df_cost_centres.rename(columns={'new_cc_code': 'cost_centre_code',
                                'Désignation CC': 'name',
                                'Entity': 'entity',
                                'Controller Name': 'controllers',
                                'CFO Name': 'cfos'},
                       inplace=True)

df_cost_centres.head()

# Empty `cost_centres` table
# ---
# Delete all from `cost_centres` table.<br>
# ON DELETE CASCADE in table cc_users.
#
# Notes:
# ---
# Check the CASCADE behaviour.

qry_delete_all_from_cost_centres = "delete from cost_centres"

run_query(qry_delete_all_from_cost_centres)

# Save cost centres data to PostgreSQL
# ---

# transform df to list
list_cost_centres = [{
    'cost_centre_code': row.cost_centre_code,
    'name': row.name,
    'entity': row.entity,
    'controllers': row.controllers,
    'cfos': row.cfos
} for row in df_cost_centres.itertuples()]

# https://hakibenita.com/fast-load-data-python-postgresql#execute-values-from-iterator

# execute_batch


def run_query_batch(qry, my_list):
    """
    Executes a query

    Args:
        qry @str: the SQL query
        my_list @list: the list
    """
    with connect() as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, qry, my_list)


qry_insert_into_cost_centres_data_ebuy = """
            INSERT INTO cost_centres VALUES (
                %(cost_centre_code)s,
                %(name)s
                )
                on conflict on constraint cost_centres_pkey
                do nothing;
        """

run_query_batch(qry_insert_into_cost_centres_data_ebuy, list_cost_centres)

# Function to extract users from cell
# ---

# There was an error showing `float object has no attribute split python`.<br>
# Solution: https://stackoverflow.com/a/52737002/7246315


def extract_users(var_user):
    """
    Extracts users from a string

    """
    user_list = str(var_user).split(",")
    user_list = [u.strip() for u in user_list]

    codes_list = [u[u.find("(")+1:u.find(")")] for u in user_list]
    fullnames_list = [u[:u.find("(")].strip() for u in user_list]

    zipped = zip(codes_list, fullnames_list)

    return zipped

# Controllers
# ---


df_controllers = pd.read_csv(FILENAME_EBUY, usecols=[3])

df_controllers.head()

df_controllers_extract = df_controllers['Controller Name'].apply(
    lambda row: list(extract_users(row)))

controllers = dict()

for i, row in enumerate(df_controllers_extract):
    for j, r in row:
        controllers[j] = r

# https://github.com/pandas-dev/pandas/issues/23848#issuecomment-440843914<br>
# This link explains why `itertuples()` would rename columns.

# ```python
# creating a dictionary for controllers with unique values
# controllers = dict()
#
# looping through the dataframe df_controllers
# to populate the dictionary
#
# for row in df_controllers.itertuples(): #https://github.com/pandas-dev/pandas/issues/23848#issuecomment-440843914
#     users_list = list(extract_users(row._1))
#     for i, r in users_list:
#         controllers[i] = r
# ```

for index, key in enumerate(controllers):
    print(key, controllers[key])
    if index == 4:
        break

# CFOs
# ---

df_cfo = pd.read_csv(FILENAME_EBUY, usecols=[4])

df_cfo.head()

df_cfo_extract = df_cfo['CFO Name'].apply(lambda row: list(extract_users(row)))

df_cfo_extract

cfo = dict()

for i, row in enumerate(df_cfo_extract):
    for (id_reseau, fullname) in row:
        cfo[id_reseau] = fullname

for index, key in enumerate(cfo):
    print(key, cfo[key])
    if index == 4:
        break

# Combine controllers and CFOs dictionaries
# ---

total_users = {**controllers, **cfo}

# transform dictionary to list
total_users_list = [{'id_reseau': key, 'fullname': val}
                    for (key, val) in total_users.items()]

# https://www.geeksforgeeks.org/iterate-over-a-set-in-python/
for index, val in enumerate(total_users_list):
    print(index, val)
    if index == 4:
        break

# 2.1 Create table for users
# ---

qry_create_table_users = """
        create table if not exists users(
        user_id_reseau text primary key,
        fullname text,
        lastname text,
        firstname text,
        email text,
        workday_id text,
        unique (user_id_reseau, fullname));
        """

run_query(qry_create_table_users)

# delete all from users
qry_delete_all_from_users = "delete from users;"

run_query(qry_delete_all_from_users)

qry_populate_users = """
            INSERT INTO users VALUES (
                %(id_reseau)s,
                %(fullname)s
                )
                on conflict on constraint users_user_id_reseau_fullname_key
                do nothing;
        """

run_query_batch(qry_populate_users, total_users_list)


# 3.1 Create the table to connect CC with users
# ---
# Loop through the dataframe with cc code, id_reseau.<br>
# Row by row, link the users in the cell with their cost centres


# previously defined dataframe
df_cost_centres.head()


# create empty list
cost_centre_controllers = []
cost_centre_cfos = []

for row in df_cost_centres.itertuples():

    controllers_list = list(extract_users(row.controllers))
    cfos_list = list(extract_users(row.cfos))

    for (id_reseau, fullname) in controllers_list:
        cost_centre_controllers.append(
            [row.cost_centre_code, id_reseau, "controller"])

    for (id_reseau, fullname) in cfos_list:
        cost_centre_cfos.append([row.cost_centre_code, id_reseau, "cfo"])

full_cost_centres_users_list = cost_centre_controllers + cost_centre_cfos

len(full_cost_centres_users_list)


full_cost_centres_users_list[:5]


# 3.2 Add cc_users table
# ---


# transform list to set
full_cost_centres_users_set = [{
    'cost_centre_code': row[0],
    'id_reseau': row[1],
    'role': row[2],
} for row in full_cost_centres_users_list]


# https://www.geeksforgeeks.org/iterate-over-a-set-in-python/
for index, row in enumerate(full_cost_centres_users_set):
    print(index, row)
    if index == 4:
        break


# Notes:
# ---
# Test if table exists before running this code.


qry_create_table_cc_users = """
        create table if not exists cc_users(
            cost_centre_code text,
            user_id_reseau text,
            role text,
            FOREIGN KEY(cost_centre_code) REFERENCES cost_centres(cost_centre_code) ON DELETE CASCADE,
            FOREIGN KEY(user_id_reseau) REFERENCES users(user_id_reseau) ON DELETE SET NULL,
            unique (cost_centre_code, user_id_reseau, role)
        );
        """
run_query(qry_create_table_cc_users)


# execute_batch
qry_populate_cc_users = """INSERT INTO cc_users VALUES (
                                       %(cost_centre_code)s,
                                       %(id_reseau)s,
                                       %(role)s
                                       )
                                       on conflict on constraint cc_users_cost_centre_code_user_id_reseau_role_key
                                       do nothing;
                                       """

run_query_batch(qry_populate_cc_users, full_cost_centres_users_set)


# effectif.csv
# ---
# This csv file contains all the users and their n1, n2 and n3.<br>
# Users in n1 column will also be in n0.<br>
# There is no need to combine n0 column with n1, n2, etc.<br>
#
# For now:
# - it's not necessary to know the supervisor of a user.
# - we only need this file to identify email addresses.
#
# Notes:
# ---
# Identify users from that file that are in n1 (and n2, n3) but not in n0.


# read the csv file and assign it to a variable
df_effectif = pd.read_csv(FILENAME_EFFECTIF, usecols=[0, 3, 4, 11, 12])
df_effectif.shape


# [drop_duplicates()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html) documentation.


# drop duplicates
df_effectif.drop_duplicates(subset=['Matricule Workday'], inplace=True)
df_effectif.shape


df_effectif.head()


# reorder columns (https://stackoverflow.com/a/55204319/7246315)
df_effectif = df_effectif[['Identifiant réseau',
                           'Matricule Workday', 'Nom', 'Prénom', 'Email']]

# rename
df_effectif.rename(columns={'Identifiant réseau': 'id_reseau',
                            'Matricule Workday': 'workday_id',
                            'Nom': 'lastname',
                            'Prénom': 'firstname',
                            'Email': 'email'},
                   inplace=True)

df_effectif.head()


# 4.1 Now we need to update the users table
# ---
# We will use the effectif csv file


effectif_list = df_effectif.values


all_users_set = [{
    'id_reseau': u[0],
    'fullname': u[2] + " " + u[3],
    'lastname': u[2],
    'firstname': u[3],
    'email': u[4],
    'workday_id': u[1]
} for u in [*effectif_list]]


for index, row in enumerate(all_users_set):
    print(row)
    if index == 4:
        break


# The code below will UPDATE all users from effectif.csv
# ---
# **It will UPDATE values for users that were already imported**<br>
# - We don't need to import all users from `effectif.csv`
# - We only need to update the current users from ebuy to get additional information.
#
# This will help us retrieve the current information in FG and compare if it needs to be updated.

# Let's filter all_users (from effectif.csv) to keep only those that we added
# ---
# This way the `UPSERT` below will only update the users we've already uploaded.


# We are creating a DataFrame with one column
# It has all the id_reseau from `total_users` which was the combination of all Controllers + CFOs
df_all_users_ebuy = pd.DataFrame([*total_users], columns=['id_reseau'])
df_all_users_ebuy.head()


# Only keep from `df_effectif` the users that were previously added
# ---
# Match is made with `id_reseau`.


df_effectif_matching = df_effectif[df_effectif['id_reseau'].isin(
    df_all_users_ebuy['id_reseau'])]


df_effectif_matching.head()


# The selection is much shorter!


len(df_effectif_matching)


# add a new column (to `df_effectif_matching`) for fullname that we will leave empty since this value is already in the table users
# (https://re-thought.com/how-to-add-new-columns-in-a-dataframe-in-pandas/)

df_effectif_matching = df_effectif_matching.assign(fullname='')


# reorder columns
df_effectif_matching = df_effectif_matching[['id_reseau',
                                             'workday_id',
                                             'fullname',
                                             'lastname',
                                             'firstname',
                                             'email']]


# convert df to list (https://datatofish.com/convert-pandas-dataframe-to-list/)
effectif_matching_list = [*df_effectif_matching.values]


effectif_matching_set = [{
    'id_reseau': u[0],
    'workday_id': u[1],
    'fullname': u[2],
    'lastname': u[3],
    'firstname': u[4],
    'email': u[5]
} for u in effectif_matching_list]


# len(set_effectif_matching)
for index, row in enumerate(effectif_matching_set):
    print(row)
    if index == 4:
        break


# UPSERT
# ---


# This is the UPSERT
qry_populate_users = """
            INSERT INTO users VALUES (
                %(id_reseau)s,
                %(fullname)s,
                %(lastname)s,
                %(firstname)s,
                %(email)s,
                %(workday_id)s
                )
                on conflict on constraint users_pkey
                do update set lastname = %(lastname)s,
                              firstname = %(firstname)s,
                              email = %(email)s,
                              workday_id = %(workday_id)s;
        """

run_query_batch(qry_populate_users, effectif_matching_set)


# Select users where email is null

qry_select_all_from_users = """
        select * from users where
        email is null;
        """

users_with_no_email = run_query_select(qry_select_all_from_users)


len(users_with_no_email)


for index, row in enumerate(users_with_no_email):
    print(row)
    if index == 4:
        break


# Export result in CSV file
# ---
# **Filename: users with no direct match in FG**<br>
# **Destination folder: work/output MM-DD/**


with open(WORK_OUTPUT + '/1_users_with_no_direct_match_in_FG.csv', mode='w', newline='', encoding='UTF8') as file:
    writer = csv.writer(file)
    writer.writerow(['code', 'user_fullname'])

    for row in users_with_no_email:
        writer.writerow([row[0], row[1]])


# Now we will import the user report from FG to match the remaining users that were not matched.<br>
# We will match them with their id_reseau.<br>
# Some will not match.<br>
# For those that don't match, we can export a csv file for us to analyze.<br>
# Once we find a match, we can update a table that will be used in the future for future "missing matches".<br>
# This table will not be updated automatically.<br>
# We can also consider that the import of "effectif.csv" is really something we do only once.<br>
# Once the table is there, we will simply match the users from the ebuy table with what we've already imported from:
# - effectif
# - and the "manual" table


df_users_FG_report = pd.read_csv(FILENAME_DELEGATES)


df_users_FG_report.columns = df_users_FG_report.columns.str.replace(
    ' ', '_').str.lower()


df_controllers_delegates = df_users_FG_report[df_users_FG_report['role']
                                              == 'Sanofi Controller']
if len(df_controllers_delegates[df_controllers_delegates['delegate_username'].isnull()]) == 0:
    print('All current controllers have a delegate')
else:
    df_controllers_delegates[df_controllers_delegates['delegate_username'].isnull(
    )]


df_cfos_delegates = df_users_FG_report[df_users_FG_report['role']
                                       == 'Sanofi CFO']
if len(df_cfos_delegates[df_cfos_delegates['delegate_username'].isnull()]) == 0:
    print('All current CFOs have a delegate')
else:
    df_cfos_delegates[df_cfos_delegates['delegate_username'].isnull()]


# Creating a dataframe of users with a `id_reseau` (`employee_id`)
# ---


df_users_FG_report_not_null = df_users_FG_report[df_users_FG_report['employee_id'].notnull(
)]
df_users_FG_report_not_null.head()


# Select users from table where email is null
# ---


# show previously found users with no emails

for index, row in enumerate(users_with_no_email):
    print(row)
    if index == 4:
        break


# Trying to find users from the report that would match with the users from the table with no email
# There is only one matching...<br>
# (ophelia.bosquet)


column_with_code = [row[0] for row in users_with_no_email]


df_users_FG_report_not_null[df_users_FG_report_not_null['employee_id'].isin(
    column_with_code)]


# Verdict
# ---
# We can see that this is not a satisfying solution.<br>
# We need to do a search based on first name + last name.<br>
# But this will need the confirmation from client that it is the correct match

# Function to search `name.surname`
# ---


# https://stackoverflow.com/a/63684031/7246315

def search(regex: str, df, case=False):
    """Search all the text columns of `df`, return rows with any matches."""
    textlikes = df.select_dtypes(include=[object, "string"])

    return df[
        textlikes.apply(
            lambda column: column.str.contains(
                regex, regex=True, case=case, na=False)
        ).any(axis=1)
    ]


# Let's create a function that will:
# - loop through `users_with_no_email`
# - remove `-ext` from the string
# - concatenate first and last name with a `.`
# - for names like "Milena Di Bella", it will remove the space in the last name
#
# For all other cases that will not match, we will use fuzzy match


def format_name(fullname):
    result = fullname

    if result[-4:] == '-ext':
        result = result[:-4]

    result = result.split(' ')

    if len(result) == 2:
        return '.'.join(result)
    elif len(result) > 2:
        return result[0] + '.' + ''.join(result[1:])
    else:
        return None


# Below we create a df with only the Controllers and CFOs from what's currently in FG (from FG report).<br>
# This will reduce the list of users to search in.


df_users_FG_report.head()


df_SP = df_users_FG_report[df_users_FG_report['role'].isin(['S-P Manager'])]
df_SP_not_closed = df_SP[df_SP['user_status'] != 'Closed']


# Notes:
# ---
# Generate a CSV file to show the Cost Centres associated to closed users

# %%timeit -r1 -n1
frames = []

for i in users_with_no_email:
    code = i[0]
    fullname = format_name(i[1])

    res = search(r"{}".format(fullname), df_SP_not_closed[['email',
                                                           'username',
                                                           'first_name',
                                                           'last_name',
                                                           'employee_id']], False)
    # res = pd.DataFrame(res).drop_duplicates()

    if not(res.empty):
        res['id_reseau_ebuy'] = code
        res['fullname_ebuy'] = fullname
        frames.append(res)


df_results = pd.concat(frames)


df_results.head()


# Checking if we only have S-P Manager role
if len(df_results[df_results['username'].apply(lambda row: row[-3:]) != '_SP']) == 0:
    print('all good')
else:
    df_results[df_results['username'].apply(lambda row: row[-3:]) != '_SP']


# df_results_SP_Only = df_results[df_results['username'].str.contains('_SP')]


# Notes:
# ---
# `employee_id` is renamed `employee_id_FG` to highlight the fact that this `id` comes from FG.


# reorder columns
df_results = df_results[['id_reseau_ebuy',
                         'fullname_ebuy',
                         'employee_id',
                         'email',
                         'username',
                         'first_name',
                         'last_name']]

# rename EmployeeID to EmployeeID_FG
df_results.rename(columns={'employee_id': 'employee_id_FG'}, inplace=True)

print("total users previously with no matches:", len(users_with_no_email))
print("number of new matches:", len(df_results))
print("remaining:", len(users_with_no_email) - len(df_results))


df_results.head()


# Export result in CSV file
# ---
# **Filename**: potential matches with users present in FG<br>
# **Destination folder**: work/output

# Notes:
# ---
# Implement a way to prevent the file to be created if there is an error:
# - variable is not defined
# - variable is empty
#
# Show a success message when the file is created.


with open(WORK_OUTPUT + '/2_potential_matches_with_users_in_FG.csv', mode='w', newline='', encoding='UTF8') as file:
    writer = csv.writer(file)
    writer.writerow(['id_reseau_ebuy',
                     'fullname_ebuy',
                     'employee_id_FG',
                     'email',
                     'username',
                     'first_name',
                     'last_name'])

    for index, row in df_results.iterrows():
        writer.writerow(row)


# Now we can update our `users` table with all the matches we've found
# ---
# The `UPSERT` below is not updating the `username` since there is no `username` field in the `users` table.

# Notes:
# ---
# We could add an extra step where these results are compared with a table that would be saved with previous run results


results_list = [*df_results.values]
type(results_list)


# looping a list
users_matching = [{
    'id_reseau_ebuy': u[0],
    'fullname_ebuy': u[1],
    'employee_id_FG': u[2],
    'email': u[3],
    'username': u[4],
    'first_name': u[5],
    'last_name': u[6],
} for u in results_list]


len(users_matching)


# Notes:
# ---
# When a query runs successfully, we need to display a success message.


qry_upsert_users = """
            INSERT INTO users VALUES (
                %(id_reseau_ebuy)s,
                %(fullname_ebuy)s,
                %(last_name)s,
                %(first_name)s,
                %(email)s,
                %(employee_id_FG)s
                )
                on conflict on constraint users_pkey
                do update set lastname = %(last_name)s,
                              firstname = %(first_name)s,
                              email = %(email)s,
                              workday_id = %(employee_id_FG)s;
        """

run_query_batch(qry_upsert_users, users_matching)


# Now let's check what's still missing
# ---


# Select users where email is null

qty_select_all_from_users_email_is_null = """
        select * from users where
        email is null;
        """

users_with_no_email_2 = run_query_select(
    qty_select_all_from_users_email_is_null)


users_with_no_email_2


# Notes:
# ---
# Some users in this list are actually in FG.
# - create a table in the database to save emails found manually in FG
#
# Once those are saved in the database, we need to do an UPSERT to update the `users` table.


# This code performs a search of ANOLASCO accross the DF Users Report
search(r'{}'.format('McKenna'), df_users_FG_report[['email',
                                                    'username',
                                                    'first_name',
                                                    'last_name',
                                                    'employee_id']], False)


# Now we can create 3 files:
# ---
# - cost centres with controllers and CFOs
# - cost centres with cfos
# - missing users
#
# We need to remember:
# - controllers have `_CONT` in the end of their usernames AND their diplay name must show `(Controller)`
# - CFOs have `_CFO` in the end of their usernames AND their diplay name must show `(CFO)`
#
# If a user needs to be a `Controller` or `CFO` but doesn't have such role(s), we need to create his `Controller` or `CFO` user account based on his main `SP` account.<br>
#
# Next step:
# - Create csv file to create missing user accounts
#
# **IMPORTANT**:<br>
# - We must create the user accounts first before making the associations.

# Query to select all cost centres with the users and their roles
# Where email is `not NULL`
# ---
# This is ultimately all the CC associations to users with an email address in `users` table.<br>
# It is important that there is an email since that means we can find the usernames.


qry_select_cost_centres_email_not_null = """
        SELECT cu.cost_centre_code, cu.user_id_reseau, cu.role,
               u.fullname, u.lastname, u.firstname, u.email, u.workday_id
        FROM cc_users cu
        LEFT JOIN users u ON cu.user_id_reseau = u.user_id_reseau
        WHERE u.email IS NOT NULL;
        """

cc_users_email_not_null = run_query_select(
    qry_select_cost_centres_email_not_null)


cc_users_email_not_null_df = pd.DataFrame(cc_users_email_not_null,
                                          columns=['cost_centre_code',
                                                   'user_id_reseau',
                                                   'role',
                                                   'fullname',
                                                   'lastname',
                                                   'firstname',
                                                   'email', 'workday_id'])


cc_users_email_not_null_df.head()


# 3.1 Export to CSV cost centres with controllers and CFOs (email not null)
# ---


type(cc_users_email_not_null_df)


with open(WORK_OUTPUT + '/3.1_cost_centres_and_users_in_FG_email_not_null.csv', mode='w', newline='', encoding='UTF8') as file:
    writer = csv.writer(file)
    writer.writerow(['cost_centre_code', 'user_id_reseau', 'role',
                     'fullname', 'lastname', 'firstname', 'email', 'workday_id'])

    for row in cc_users_email_not_null:
        writer.writerow(row)


# 3.2 Export to CSV cost centres with controllers and CFOs (email IS NULL)
# ---


# Select cost centre code, email, username, role

qry_select_cost_centres_email_is_null = """
        SELECT cu.cost_centre_code, cu.user_id_reseau, cu.role,
               u.fullname, u.lastname, u.firstname, u.email, u.workday_id
        FROM cc_users cu
        LEFT JOIN users u ON cu.user_id_reseau = u.user_id_reseau
        WHERE u.email IS NULL;
        """

cc_users_email_is_null = run_query_select(
    qry_select_cost_centres_email_is_null)


pd.DataFrame(cc_users_email_is_null, columns=['cost_centre_code', 'user_id_reseau', 'role',
                                              'fullname', 'lastname', 'firstname', 'email', 'workday_id']).head()


with open(WORK_OUTPUT + '3.2_cost_centres_and_users_in_FG_email_is_null.csv', mode='w', newline='', encoding='UTF8') as file:
    writer = csv.writer(file)
    writer.writerow(['cost_centre_code', 'user_id_reseau', 'role',
                     'fullname', 'lastname', 'firstname', 'email', 'workday_id'])

    for row in cc_users_email_is_null:
        writer.writerow(row)


# Create a CSV with the users and their 'extension' (_CONT or _CFO)
# ---

# Notes:
# ---
# - modify the code below
#
# Ultimately we want to find the usernames we want to create.


qry_select_users_with_email = """
        SELECT u.email, cu.role, 
        case
            when cu.role = 'controller'
                then u.email || '_CONT'
            else u.email || '_CFO'
        end username
        FROM cc_users cu
        LEFT JOIN users u ON cu.user_id_reseau = u.user_id_reseau
        WHERE u.email IS NOT NULL;
        """

users_with_email = run_query_select(qry_select_users_with_email)


df_users_with_email = pd.DataFrame(users_with_email,
                                   columns=['email', 'role', 'username'])

df_users_with_email = df_users_with_email.drop_duplicates()

len(df_users_with_email)


# Let's check which ones are already in FG with the correct role and output only the ones MATCHING
# ---


df_users_matching = df_users_FG_report[df_users_FG_report['username'].isin(
    df_users_with_email['username'].values)]


len(df_users_matching)


# https://dfrieds.com/data-analysis/pivot-table-python-pandas.html
# pivot table
# pt_employee_id = pd.pivot_table(data=df_users_matching, index='username', values='employee_id', aggfunc='count')


# Let's now fetch the ones that are MISSING
# ---


df_users_missing = df_users_with_email[
    df_users_with_email['username'].isin(
        [*df_users_matching['username']]) == False
]

df_users_missing = df_users_missing.drop_duplicates()

len(df_users_missing)


df_users_missing


# Export list of users to create with their respective roles
# ---


with open(WORK_OUTPUT + '/4_users_roles_to_create.csv', mode='w', newline='', encoding='UTF8') as file:
    writer = csv.writer(file)
    writer.writerow(['email', 'role', 'username'])

    for index, row in df_users_missing.iterrows():
        writer.writerow(row)


# Reading the new reports
# ---
# - **FILENAME_CC_ALL**: all the cost centres in FG
# - **FILENAME_CC_Usernames**: all the cost centre with usernames


# read the csv file and assign it to a variable
df_cost_centres_all = pd.read_csv(FILENAME_CC_ALL, sep='|')

df_cost_centres_usernames = pd.read_csv(FILENAME_CC_Usernames)


# rename columns
df_cost_centres_all.rename(columns={'Cost Centre': 'cost_centre',
                                    'Cost Centre Code': 'cost_centre_code',
                                    'Cost Centre Owner': 'cost_centre_owner',
                                    'Cost Centre Owner Email': 'cost_centre_owner_email',
                                    'CC Currency': 'currency',
                                    'Company Code': 'company_code',
                                    'Cost Centre Status': 'cost_centre_status'},
                           inplace=True)

# rename columns
df_cost_centres_usernames.rename(columns={'Cost Centre': 'cost_centre',
                                          'Cost Centre Code': 'cost_centre_code',
                                          'Username': 'username',
                                          'Role': 'role',
                                          'Cost Centre Status': 'cost_centre_status'},
                                 inplace=True)


df_cost_centres_all.head()


df_cost_centres_usernames.head()


# Check which cc + usernames are already in FG
# ---


# Here we're creating a key
# key is the combination of cc + usernames

key_cc_usernames_FG = df_cost_centres_usernames.apply(
    lambda row: row['cost_centre_code'] + '_' + row['username'],
    axis=1)
key_cc_usernames_FG.head()


def create_username(email, role):
    if role == 'controller':
        return email + '_CONT'
    elif role == 'cfo':
        return email + '_CFO'
    else:
        return email + '_' + role


# ---
# `cc_users_email_not_null_df` is the result from a query retrieving all users with an email.<br>
# This table doesn't have a username field and we need to create the usernames based on the `role` field.
# ---


# key_cc_usernames_ebuy is a list of keys
# taken from cc_users_email_not_null_df
# which is the result of the query
# which is all the cc from the weekly file and usernames
key_cc_usernames_ebuy = cc_users_email_not_null_df.apply(
    lambda row: row['cost_centre_code'] + '_' +
    create_username(row['email'], row['role']),
    axis=1)


# Notes:
# ---
# Code below is slow.<br>
# Perhaps this is something that could be parallelised.


# here we're using invert=True to keep only
# the keys from ebuy (key_cc_usernames_ebuy)
# that are not in
# key_cc_usernames_FG
upload_cc_existing_usernames_df = cc_users_email_not_null_df[
    np.isin(key_cc_usernames_ebuy,
            key_cc_usernames_FG,
            invert=True)]


# here we're creating a copy otherwise we're going to get an error
upload_cc_existing_usernames_df_copy = upload_cc_existing_usernames_df.copy()


upload_cc_existing_usernames_df_copy['username'] = upload_cc_existing_usernames_df.apply(
    lambda row: create_username(row['email'], row['role']),
    axis=1)


upload_cc_existing_usernames_df_copy.head()


# Create upload file of combinations (cc + username) not in FG:
# ---
# cc + usernames (usernames present in FG)


with open(WORK_OUTPUT + '5_upload_cc_existing_usernames.csv', mode='w', newline='', encoding='UTF8') as file:
    writer = csv.writer(file)
    writer.writerow(['cost_centre_code', 'username'])

    for row in upload_cc_existing_usernames_df_copy.itertuples():
        writer.writerow([row.cost_centre_code, row.username])


# Check if the username from the previous generated upload is in FG
# ---


usernames_to_check = upload_cc_existing_usernames_df_copy.loc[:, [
    'role', 'email', 'username']].drop_duplicates()


usernames_not_existing = usernames_to_check[
    np.isin(usernames_to_check['username'],
            df_users_FG_report['username'],
            invert=True)].drop_duplicates()


usernames_not_existing


# Here we're going to retrieve just the emails
# ---


usernames_not_existing_emails_df = usernames_not_existing['email'].drop_duplicates(
)


emails_report_FG = df_users_FG_report['email'].drop_duplicates()


# ---
# **Here we're checking if the emails from `usernames_not_existing_emails_df` are in the report with all the users.**
#
# ---
#
# If there's a match, we save the email.<br>
# The goal is to compare the emails of what we need to create<br>
# versus the emails available from the report.<br>
# If there is a discrepancy (email missing), then that would be a totally new user to create.


emails_to_copy = []

for row in usernames_not_existing_emails_df:
    if (row in [*emails_report_FG.values]):
        emails_to_copy.append(row)


emails_to_copy


# Retrieving information from FG about users
# ---


# read the csv file and assign it to a variable
df_user_info_FG = pd.read_csv(FILENAME_USER_INFO)


df_user_info_FG.columns


# rename columns
df_user_info_FG.rename(columns={'Username': 'username',
                                'First Name': 'first_name',
                                'Last Name': 'last_name',
                                'Display Name': 'display_name',
                                'Email': 'email',
                                'Employee ID': 'employee_id',
                                'User Title': 'user_title',
                                'Role': 'role',
                                'Primary Business Unit Code': 'primary_bu_code',
                                'Primary Supervisor Username': 'primary_supervisor_username',
                                'Time Zone': 'time_zone',
                                'Currency': 'currency',
                                '[SA] EUR': 'signature_authority',
                                'User Status': 'user_status'},
                       inplace=True)


df_user_info_FG.columns


# Keep usernames we want to create
# ---
# For each row from `usernames_not_existing`, we want to create a username

# For controllers
# ---


usernames_not_existing_CONT = usernames_not_existing[usernames_not_existing['role'] == 'controller']


df_CONT_to_create = df_user_info_FG[df_user_info_FG['email'].isin(
    usernames_not_existing_CONT['email'])]


df_CONT_to_create_copy = df_CONT_to_create.copy()


df_CONT_to_create_copy['role'] = 'Sanofi Controller'
df_CONT_to_create_copy['username'] = df_CONT_to_create_copy['email'] + '_CONT'
df_CONT_to_create_copy['display_name'] = df_CONT_to_create_copy['display_name'] + \
    ' (Controller)'

df_CONT_to_create_copy = df_CONT_to_create_copy.reset_index(drop=True)


df_CONT_to_create_copy


# For CFOs
# ---


usernames_not_existing_CFO = usernames_not_existing[usernames_not_existing['role'] == 'cfo']


df_CFO_to_create = df_user_info_FG[df_user_info_FG['email'].isin(
    usernames_not_existing_CFO['email'])]


df_CFO_to_create_copy = df_CFO_to_create.copy()


df_CFO_to_create_copy['role'] = 'Sanofi CFO'
df_CFO_to_create_copy['username'] = df_CFO_to_create_copy['email'] + '_CFO'
df_CFO_to_create_copy['display_name'] = df_CFO_to_create_copy['display_name'] + \
    ' (CFO)'

df_CFO_to_create_copy = df_CFO_to_create_copy.reset_index(drop=True)


df_CFO_to_create_copy


df_total_users_to_create = df_CONT_to_create_copy.append(
    df_CFO_to_create_copy, ignore_index=True)


# Here we're adding all the other columns necessary for the upload.
#
# ---


df_total_users_to_create['modification'] = 'A'
df_total_users_to_create['broadcast'] = 'Email'
df_total_users_to_create['notification'] = 'Email'
df_total_users_to_create['login_type'] = 'Both'
# This is very important
df_total_users_to_create['cost_centre_access'] = 'Associate'
df_total_users_to_create['business_unit_access'] = 'ALL'
df_total_users_to_create['site_access'] = 'ALL'


df_total_users_to_create.columns


# delete column that is not needed for the upload
del df_total_users_to_create['user_status']


# reorder columns (https://stackoverflow.com/a/55204319/7246315)
df_total_users_to_create = df_total_users_to_create[['modification',
                                                     'username',
                                                     'first_name',
                                                     'last_name',
                                                     'display_name',
                                                     'email',
                                                     'employee_id',
                                                     'user_title',
                                                     'role',
                                                     'primary_bu_code',
                                                     'primary_supervisor_username',
                                                     'time_zone',
                                                     'currency',
                                                     'broadcast',
                                                     'notification',
                                                     'login_type',
                                                     'cost_centre_access',
                                                     'business_unit_access',
                                                     'site_access',
                                                     'signature_authority']]


# rename columns
df_total_users_to_create.rename(columns={'username': 'Username',
                                         'first_name': 'First Name',
                                         'last_name': 'Last Name',
                                         'display_name': 'Display Name',
                                         'email': 'Email',
                                         'employee_id': 'Employee ID',
                                         'user_title': 'Title',
                                         'role': 'Role Name',
                                         'primary_bu_code': 'Primary Business Unit',
                                         'primary_supervisor_username': 'Primary Supervisor Username',
                                         'time_zone': 'Time Zone',
                                         'currency': 'Currency',
                                         'signature_authority': '[SA] EUR',
                                         'user_status': '',
                                         'modification': 'Modification Type',
                                         'broadcast': 'Broadcast',
                                         'notification': 'Notification',
                                         'login_type': 'Login Type',
                                         'cost_centre_access': 'Cost Centre Access',
                                         'business_unit_access': 'Business Unit Access',
                                         'site_access': 'Site Access'},
                                inplace=True)


df_total_users_to_create.columns


# 6. Now we can create the upload file to create those usernames
# ---


with open(WORK_OUTPUT + '/6_upload_new_usernames.csv', mode='w', newline='', encoding='UTF8') as file:
    writer = csv.writer(file)
    writer.writerow(['Modification Type', 'Username', 'First Name', 'Last Name',
                     'Display Name', 'Email', 'Employee ID', 'Title', 'Role Name',
                     'Primary Business Unit', 'Primary Supervisor Username', 'Time Zone',
                     'Currency', 'Broadcast', 'Notification', 'Login Type',
                     'Cost Centre Access', 'Business Unit Access', 'Site Access',
                     '[SA] EUR'])

    for row in df_total_users_to_create.itertuples():
        writer.writerow(row)


# 7. Now we need to take care of the delegates
# ---
# And perhaps check if there is any `cfo` or `controller` not attached to their main `sp` account.


# 8. Now we need to remove all `controllers` and `CFOs` that are in FG but not in the weekly file.
# ---

# Let's retrieve first info from the weekly file.
# ---


# all cc and usernames

qry_select_all_cc_and_users = """
        SELECT cu.cost_centre_code, cu.user_id_reseau, cu.role,
               u.fullname, u.lastname, u.firstname, u.email, u.workday_id
        FROM cc_users cu
        LEFT JOIN users u ON cu.user_id_reseau = u.user_id_reseau
        WHERE u.email IS NOT NULL;
        """

all_cc_and_users = run_query_select(qry_select_all_cc_and_users)


all_cc_and_users_df = pd.DataFrame(all_cc_and_users, columns=['cost_centre_code', 'id_reseau', 'role', 'fullname',
                                                              'last_name', 'first_name', 'email', 'workday_id'])


# len(all_cc_and_users_df)


#all_cc_and_users_email_not_null_df = all_cc_and_users_df[all_cc_and_users_df['email'].notnull()]


# len(all_cc_and_users_email_not_null_df)


all_cc_and_users_df_copy = all_cc_and_users_df.copy()


all_cc_and_users_df_copy['username'] = all_cc_and_users_df_copy.apply(
    lambda row: create_username(row['email'], row['role']),
    axis=1)


all_cc_and_users_df_copy['cc_username'] = all_cc_and_users_df_copy.apply(lambda row: row['cost_centre_code'] + '_' + row['username'],
                                                                         axis=1)


key_cc_usernames = all_cc_and_users_df_copy['cc_username'].values


type(key_cc_usernames)


# Now let's retrieve the same information from what we have in FG
# ---


# this comes from
# df_cost_centres_usernames = pd.read_csv(FILENAME_CC_Usernames)
df_cost_centres_usernames.head()


df_cost_centres_usernames_FG_copy = df_cost_centres_usernames.copy()


df_cost_centres_usernames_FG_copy.head()


df_cost_centres_usernames_FG_copy['cc_username'] = df_cost_centres_usernames_FG_copy['cost_centre_code'].values + \
    '_' + df_cost_centres_usernames_FG_copy['username'].values


df_cost_centres_usernames_FG_copy.head()


# Now we can find the elements that are in FG but not in the weekly file.
# ---
# We need to remove the cc and users associations that are present in FG but not in the weekly file.


associations_to_delete = df_cost_centres_usernames_FG_copy[
    df_cost_centres_usernames_FG_copy['cc_username'].isin(
        key_cc_usernames) == False
]


# Export that list of associations to a CSV file
# ---


associations_to_delete.columns


# reorder columns
associations_to_delete = associations_to_delete[
    ['cost_centre_code', 'cost_centre', 'username',
        'role', 'cost_centre_status', 'cc_username']
]


with open(WORK_OUTPUT + '/7_cc_usernames_associations_to_delete.csv', mode='w', newline='', encoding='UTF8') as file:
    writer = csv.writer(file)
    writer.writerow(['cost_centre_code',
                     'cost_centre',
                     'username',
                     'role',
                     'cost_centre_status',
                     'cc_username'])

    for index, row in associations_to_delete.iterrows():
        writer.writerow(row)


datetime_2 = datetime.now()
print(str(datetime_2))

total_time = datetime_2-datetime_1
print(str(total_time))

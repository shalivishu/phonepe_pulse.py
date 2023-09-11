import pandas as pd
import mysql.connector as sql
from mysql.connector import Error
import streamlit as st
import plotly.express as px
import os
import json
from git.repo.base import Repo





# Set an environment variable to suppress warning messages
os.environ["GIT_PYTHON_REFRESH"] = "quiet"

# Specify the existing local repository path
local_path = r"C:\Users\shali\OneDrive\Desktop\pulse-clone"

# Open the existing repository
repo = Repo(local_path)

# Pull the latest changes from the remote repository
repo.remotes.origin.pull()

print("Repository updated successfully.")



import mysql.connector as sql

# Define your database connection parameters
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "Phonepe_pulse"
}

# Connect to the MySQL server without specifying a database
db_connection = sql.connect(host=db_config["host"], user=db_config["user"], password=db_config["password"])

# Create a cursor
db_cursor = db_connection.cursor()

try:
    db_cursor.execute("SHOW DATABASES")
    databases = db_cursor.fetchall()

    for database in databases:
        if database[0] == db_config["database"]:
            # Delete the existing database
            db_cursor.execute(f"DROP DATABASE {db_config['database']}")
            print(f"Existing database '{db_config['database']}' deleted.")

    # Create the new database
    db_cursor.execute(f"CREATE DATABASE {db_config['database']}")
    print(f"New database '{db_config['database']}' created.")

except sql.Error as err:
    print("Error:", err)

finally:
    # Close the cursor and connection
    db_cursor.close()
    db_connection.close()




def aggregated_transcation_state():
    # Specify the base path to the data directory
    path1 = r"C:\Users\shali\DT 10 DT 11\pulse\data\aggregated\transaction\country\india\state"

    # Initialize a dictionary to store column data
    columns1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],
                'Transaction_amount': []}


    # Iterate through the subdirectories (states)
    for state in os.listdir(path1):
        cur_state = os.path.join(path1 , state)
        
        # Check if the path exists and is a directory
        if os.path.isdir(cur_state):
            # Iterate through the subdirectories (years)
            for year in os.listdir(cur_state):
                cur_year = os.path.join(cur_state, year)
                
                # Check if the path exists and is a directory
                if os.path.isdir(cur_year):
                    # Iterate through the JSON files
                    for file in os.listdir(cur_year):
                        if file.endswith('.json'):
                            cur_file = os.path.join(cur_year, file)
                            data = open(cur_file, 'r')
                            A = json.load(data)
                            
                            for i in A['data']['transactionData']:
                                name = i['name']
                                count = i['paymentInstruments'][0]['count']
                                amount = i['paymentInstruments'][0]['amount']
                                columns1['Transaction_type'].append(name)
                                columns1['Transaction_count'].append(count)
                                columns1['Transaction_amount'].append(amount)
                                columns1['State'].append(state)
                                columns1['Year'].append(year)
                                columns1['Quarter'].append(int(file.strip('.json')))
                            
                            data.close()

    # Create the DataFrame
    df_agg_trans = pd.DataFrame(columns1)
    return df_agg_trans


# Define your database connection parameters
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "Phonepe_pulse"
}

def insert_agg_trans_data(dataframe, mycursor, mydb):
    try:
        # Creating agg_trans table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS agg_trans (
                State VARCHAR(255),
                Year INT,
                Quarter INT,
                Transaction_type VARCHAR(255),
                Transaction_count INT,
                Transaction_amount DOUBLE
            )
        """
        mycursor.execute(create_table_query)

        for i, row in dataframe.iterrows():
            insert_query = "INSERT INTO agg_trans (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"
            data_tuple = (
                row["State"],
                row["Year"],
                row["Quarter"],
                row["Transaction_type"],
                row["Transaction_count"],
                row["Transaction_amount"]
            )
            mycursor.execute(insert_query, data_tuple)

        mydb.commit()
        print("Data inserted into 'agg_trans' table successfully.")

    except Error as err:
        print("Error:", err)
        mydb.rollback()

    finally:
        mycursor.close()
        mydb.close()

# Load your aggregated data into dataframes
df_agg_trans = aggregated_transcation_state()

# Establish database connection
mydb = sql.connect(**db_config)
mycursor = mydb.cursor()

# Call the function to insert data into the table
insert_agg_trans_data(df_agg_trans, mycursor, mydb)



def aggregated_user_state():
    # Specify the path to the directory containing JSON files for aggregated user data
    path2 = r"C:\Users\shali\DT 10 DT 11\pulse\data\aggregated\user\country\india\state"

    agg_user_list = os.listdir(path2)

    columns2 = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'Count': [],
                'Percentage': []}
    for state in agg_user_list:
        cur_state = os.path.join(path2, state)
        agg_year_list = os.listdir(cur_state)
        
        for year in agg_year_list:
            cur_year = os.path.join(cur_state, year)
            agg_file_list = os.listdir(cur_year)

            for file in agg_file_list:
                cur_file = os.path.join(cur_year, file)
                with open(cur_file, 'r') as data:
                    B = json.load(data)
                    try:
                        for i in B["data"]["usersByDevice"]:
                            brand_name = i["brand"]
                            counts = i["count"]
                            percents = i["percentage"]
                            columns2["Brands"].append(brand_name)
                            columns2["Count"].append(counts)
                            columns2["Percentage"].append(percents)
                            columns2["State"].append(state)
                            columns2["Year"].append(year)
                            columns2["Quarter"].append(int(file.strip('.json')))
                    except:
                        pass
    df_agg_user = pd.DataFrame(columns2)
    return df_agg_user


import pandas as pd
import mysql.connector as sql
from mysql.connector import Error

# Define your database connection parameters
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "Phonepe_pulse"
}

def insert_agg_user_data(dataframe, mycursor, mydb):
    try:
        # Creating agg_user table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS agg_user (
                State VARCHAR(255),
                Year INT,
                Quarter INT,
                Brands VARCHAR(255),
                Count INT,
                Percentage DOUBLE
            )
        """
        mycursor.execute(create_table_query)

        for i, row in dataframe.iterrows():
            insert_query = "INSERT INTO agg_user (State, Year, Quarter, Brands, Count, Percentage) VALUES (%s, %s, %s, %s, %s, %s)"
            data_tuple = (
                row["State"],
                row["Year"],
                row["Quarter"],
                row["Brands"],
                row["Count"],
                row["Percentage"]
            )
            mycursor.execute(insert_query, data_tuple)

        mydb.commit()
        print("Data inserted into 'agg_user' table successfully.")

    except Error as err:
        print("Error:", err)
        mydb.rollback()

    finally:
        mycursor.close()
        mydb.close()

# Load your aggregated user data into a DataFrame
df_agg_user = aggregated_user_state()  # Implement this function to load the data

# Establish database connection
mydb = sql.connect(**db_config)
mycursor = mydb.cursor()

# Call the function to insert data into the table
insert_agg_user_data(df_agg_user, mycursor, mydb)


    
def map_transcation_state():
    # Specify the path to the directory containing JSON files for map transactions data
    path3 = r"C:\Users\shali\DT 10 DT 11\pulse\data\map\transaction\hover\country\india\state"

    map_trans_list = os.listdir(path3)

    columns3 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [],
                'Amount': []}

    for state in map_trans_list:
        cur_state = os.path.join(path3, state)
        map_year_list = os.listdir(cur_state)
        
        for year in map_year_list:
            cur_year = os.path.join(cur_state, year)
            map_file_list = os.listdir(cur_year)
            
            for file in map_file_list:
                cur_file = os.path.join(cur_year, file)
                with open(cur_file, 'r') as data:
                    C = json.load(data)
                
                    for i in C["data"]["hoverDataList"]:
                        district = i["name"]
                        count = i["metric"][0]["count"]
                        amount = i["metric"][0]["amount"]
                        columns3["District"].append(district)
                        columns3["Count"].append(count)
                        columns3["Amount"].append(amount)
                        columns3['State'].append(state)
                        columns3['Year'].append(year)
                        columns3['Quarter'].append(int(file.strip('.json')))
                    
    df_map_trans = pd.DataFrame(columns3)
    return df_map_trans


# Define your database connection parameters
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "Phonepe_pulse"
}

def insert_map_trans_data(dataframe, mycursor, mydb):
    try:
        # Creating map_trans table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS map_trans (
                State VARCHAR(255),
                Year INT,
                Quarter INT,
                District VARCHAR(255),
                Count INT,
                Amount DOUBLE
            )
        """
        mycursor.execute(create_table_query)

        for i, row in dataframe.iterrows():
            insert_query = "INSERT INTO map_trans (State, Year, Quarter, District, Count, Amount) VALUES (%s, %s, %s, %s, %s, %s)"
            data_tuple = (
                row["State"],
                row["Year"],
                row["Quarter"],
                row["District"],
                row["Count"],
                row["Amount"]
            )
            mycursor.execute(insert_query, data_tuple)

        mydb.commit()
        print("Data inserted into 'map_trans' table successfully.")

    except Error as err:
        print("Error:", err)
        mydb.rollback()

    finally:
        mycursor.close()
        mydb.close()

# Load your map transaction data into a DataFrame
df_map_trans = map_transcation_state()  # Implement this function to load the data

# Establish database connection
mydb = sql.connect(**db_config)
mycursor = mydb.cursor()

# Call the function to insert data into the table
insert_map_trans_data(df_map_trans, mycursor, mydb)



def map_user_state():
    # Specify the path to the directory containing JSON files for map user data
    path4 = r"C:\Users\shali\DT 10 DT 11\pulse\data\map\user\hover\country\india\state"

    map_user_list = os.listdir(path4)

    columns4 = {"State": [], "Year": [], "Quarter": [], "District": [],
                "RegisteredUser": [], "AppOpens": []}

    for state in map_user_list:
        cur_state = os.path.join(path4, state)
        map_year_list = os.listdir(cur_state)
        
        for year in map_year_list:
            cur_year = os.path.join(cur_state, year)
            map_file_list = os.listdir(cur_year)
            
            for file in map_file_list:
                cur_file = os.path.join(cur_year, file)
                with open(cur_file, 'r') as data:
                    D = json.load(data)
                
                    for district, values in D["data"]["hoverData"].items():
                        registereduser = values["registeredUsers"]
                        appOpens = values['appOpens']
                        columns4["District"].append(district)
                        columns4["RegisteredUser"].append(registereduser)
                        columns4["AppOpens"].append(appOpens)
                        columns4['State'].append(state)
                        columns4['Year'].append(year)
                        columns4['Quarter'].append(int(file.strip('.json')))
                    
    df_map_user = pd.DataFrame(columns4)
    return df_map_user


# Define your database connection parameters
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "Phonepe_pulse"
}

def insert_map_user_data(dataframe, mycursor, mydb):
    try:
        # Creating map_user table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS map_user (
                State VARCHAR(255),
                Year INT,
                Quarter INT,
                District VARCHAR(255),
                RegisteredUser INT,
                AppOpens INT
            )
        """
        mycursor.execute(create_table_query)

        for i, row in dataframe.iterrows():
            insert_query = "INSERT INTO map_user (State, Year, Quarter, District, RegisteredUser, AppOpens) VALUES (%s, %s, %s, %s, %s, %s)"
            data_tuple = (
                row["State"],
                row["Year"],
                row["Quarter"],
                row["District"],
                row["RegisteredUser"],
                row["AppOpens"]
            )
            mycursor.execute(insert_query, data_tuple)

        mydb.commit()
        print("Data inserted into 'map_user' table successfully.")

    except Error as err:
        print("Error:", err)
        mydb.rollback()

    finally:
        mycursor.close()
        mydb.close()

# Load your map user data into a DataFrame
df_map_user = map_user_state()  # Implement this function to load the data

# Establish database connection
mydb = sql.connect(**db_config)
mycursor = mydb.cursor()

# Call the function to insert data into the table
insert_map_user_data(df_map_user, mycursor, mydb)



def top_transcation_state():

    path5 = r"C:\Users\shali\DT 10 DT 11\pulse\data\top\transaction\country\india\state"

    top_trans_list = os.listdir(path5)
    columns5 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_count': [], 'Transaction_amount': []}

    for state in top_trans_list:
        cur_state = os.path.join(path5, state)
        top_year_list = os.listdir(cur_state)
        
        for year in top_year_list:
            cur_year = os.path.join(cur_state, year)
            top_file_list = os.listdir(cur_year)
            
            for file in top_file_list:
                cur_file = os.path.join(cur_year, file)
                with open(cur_file, 'r') as data:
                    json_data = json.load(data)
                    
                    # Extract data from JSON based on your actual structure
                    # Adjust these lines based on the actual structure of your JSON files
                    transaction_data = json_data['data']['pincodes']  # Example key structure
                    for entry in transaction_data:
                        count = entry['metric']['count']
                        amount = entry['metric']['amount']
                        columns5['Transaction_count'].append(count)
                        columns5['Transaction_amount'].append(amount)
                        columns5['State'].append(state)
                        columns5['Year'].append(year)
                        columns5['Quarter'].append(int(file.strip('.json')))
                    
    df_top_trans = pd.DataFrame(columns5)
    return df_top_trans


# Define your database connection parameters
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "Phonepe_pulse"
}

def insert_top_trans_data(dataframe, mycursor, mydb):
    try:
        # Creating top_trans table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS top_trans (
                State VARCHAR(255),
                Year INT,
                Quarter INT,
                Transaction_count INT,
                Transaction_amount DOUBLE
            )
        """
        mycursor.execute(create_table_query)

        for i, row in dataframe.iterrows():
            insert_query = "INSERT INTO top_trans (State, Year, Quarter, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s)"
            data_tuple = (
                row["State"],
                row["Year"],
                row["Quarter"],
                row["Transaction_count"],
                row["Transaction_amount"]
            )
            mycursor.execute(insert_query, data_tuple)

        mydb.commit()
        print("Data inserted into 'top_trans' table successfully.")

    except Error as err:
        print("Error:", err)
        mydb.rollback()

    finally:
        mycursor.close()
        mydb.close()

# Load your top transaction data into a DataFrame
df_top_trans = top_transcation_state()  # Implement this function to load the data

# Establish database connection
mydb = sql.connect(**db_config)
mycursor = mydb.cursor()

# Call the function to insert data into the table
insert_top_trans_data(df_top_trans, mycursor, mydb)



def top_user_state():
    # Specify the path to the directory containing JSON files for top users data
    path6 = r"C:\Users\shali\DT 10 DT 11\pulse\data\top\user\country\india\state"

    top_user_list = os.listdir(path6)
    columns6 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],
                'RegisteredUsers': []}

    for state in top_user_list:
        cur_state = os.path.join(path6, state)
        top_year_list = os.listdir(cur_state)
        
        for year in top_year_list:
            cur_year = os.path.join(cur_state, year)
            top_file_list = os.listdir(cur_year)
            
            for file in top_file_list:
                cur_file = os.path.join(cur_year, file)
                with open(cur_file, 'r') as data:
                    F = json.load(data)
                
                    for i in F['data']['pincodes']:
                        name = i['name']
                        registeredUsers = i['registeredUsers']
                        columns6['Pincode'].append(name)
                        columns6['RegisteredUsers'].append(registeredUsers)
                        columns6['State'].append(state)
                        columns6['Year'].append(year)
                        columns6['Quarter'].append(int(file.strip('.json')))
    df_top_user = pd.DataFrame(columns6)
    return df_top_user


# Define your database connection parameters
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "Phonepe_pulse"
}

def insert_top_user_data(dataframe, mycursor, mydb):
    try:
        # Creating top_user table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS top_user (
                State VARCHAR(255),
                Year INT,
                Quarter INT,
                Pincode INT,
                RegisteredUsers INT
            )
        """
        mycursor.execute(create_table_query)

        for i, row in dataframe.iterrows():
            insert_query = "INSERT INTO top_user (State, Year, Quarter, Pincode, RegisteredUsers) VALUES (%s, %s, %s, %s, %s)"
            data_tuple = (
                row["State"],
                row["Year"],
                row["Quarter"],
                row["Pincode"],
                row["RegisteredUsers"]
            )
            mycursor.execute(insert_query, data_tuple)

        mydb.commit()
        print("Data inserted into 'top_user' table successfully.")

    except Error as err:
        print("Error:", err)
        mydb.rollback()

    finally:
        mycursor.close()
        mydb.close()

# Load your top user data into a DataFrame
df_top_user = top_user_state()  # Implement this function to load the data

# Establish database connection
mydb = sql.connect(**db_config)
mycursor = mydb.cursor()

# Call the function to insert data into the table
insert_top_user_data(df_top_user, mycursor, mydb)




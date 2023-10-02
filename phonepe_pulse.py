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



import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_option_menu import option_menu
from PIL import Image




# Load CSV files
csv_files = {
    'File 1': r'C:\Users\shali\OneDrive\Desktop\test\agg_trans.csv',
    'File 2': r'C:\Users\shali\OneDrive\Desktop\test\agg_user.csv',
    'File 3': r'C:\Users\shali\OneDrive\Desktop\test\map_trans.csv',
    'File 4': r'C:\Users\shali\OneDrive\Desktop\test\map_user.csv',
    'File 5': r'C:\Users\shali\OneDrive\Desktop\test\top_trans.csv',
    'File 6': r'C:\Users\shali\OneDrive\Desktop\test\top_user.csv'
}

# Load all CSV files
data = {key: pd.read_csv(path) for key, path in csv_files.items()}


def load_data(file_path):
    return pd.read_csv(file_path)

# Define a style for Garamond-Bold font with color and font size
garamond_bold_style = "font-family: Garamond, sans-serif; font-weight: bold; color: violet; font-size: 30px;"
garamond_bold_style2 = "font-family: Garamond, sans-serif; font-weight: bold; color: white; font-size: 25px;"

# Main Streamlit function
def main():
        st.set_page_config(
            page_title="Phonepe Pulse Data Visualization",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded",
            menu_items={
                "About": "<h1 style='" + garamond_bold_style + "'>About:</h1> The dashboard is created for user-friendly data visualization. Data has been cloned from the Phonepe Pulse Github Repository."
            }
        )
        
        st.markdown(f"<h1 style='{garamond_bold_style}'>Phonepe Pulse Dashboard</h1>", unsafe_allow_html=True)
        st.sidebar.markdown(f"<h2 style='{garamond_bold_style2}'>Hello!!!</h2>", unsafe_allow_html=True)

         # Load the image with PIL
        image_path1 = "C:/Users/shali/OneDrive/Desktop/test/phonepe_image.jpeg"
        pil_image = Image.open(image_path1)

        # Specify the image width (adjust as needed)
        image_width = 1000

        # Display the PIL image with the specified width
        st.image(pil_image, width=image_width)
        
        # Creating option menu in the side bar
        with st.sidebar:
            selected = option_menu("Menu", ["Home","Top Charts","Explore Data","About"], 
                        icons=["house","graph-up-arrow","bar-chart-line", "exclamation-circle"],
                        menu_icon= "menu-button-wide",
                        default_index=0,
                        styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "#6F36AD"},
                                "nav-link-selected": {"background-color": "#6F36AD"}})
        
        # Show content based on the selected menu item
        if selected == "Home":
            st.markdown("<h1 style='" + garamond_bold_style + "'>Welcome to the PhonePe Pulse Data Visualization Dashboard!</h1>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>Explore the pulse of digital transactions and user activity on the PhonePe platform.</p>", unsafe_allow_html=True)
        
            # Project Overview
            st.markdown("<h1 style='" + garamond_bold_style + "'>Project Overview</h1>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>The PhonePe Pulse Data Visualization Dashboard offers a comprehensive view of digital transactions and user behavior on the PhonePe platform. With interactive charts and insights, you can gain valuable insights into trends, patterns, and top performers.</h1>", unsafe_allow_html=True)
            
            # Dashboard Features
            st.markdown("<h1 style='" + garamond_bold_style + "'>Dashboard Features</h1>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>1. **Top Charts:** Discover the top performing transactions and users.</h1>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>2. **Explore Data:** Dive into detailed data exploration, filtering, and visualization.</h1>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>3. **About:** Learn more about the project and data sources.</h1>", unsafe_allow_html=True)
            
            # Data Sources
            st.markdown("<h1 style='" + garamond_bold_style + "'>Data Sources</h1>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>The data for this dashboard has been sourced from the PhonePe Pulse GitHub repository. It includes aggregated and mapped transaction and user data.</h1>", unsafe_allow_html=True)
            
            # Get Started
            st.markdown("<h1 style='" + garamond_bold_style + "'>Get Started</h1>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>To get started, simply select a menu item from the sidebar to navigate through the dashboard. Explore charts, filter data, and gain insights into the PhonePe platform's digital pulse.</h1>", unsafe_allow_html=True)
        
        # MENU 2 - TOP CHARTS
        if selected == "Top Charts":
            st.markdown("<h1 style='" + garamond_bold_style + "'>Top Charts</h1>", unsafe_allow_html=True)
            Type = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))
            colum1, colum2 = st.columns([1, 1.5], gap="large")
            
            with colum1:
                Year = st.slider("**Year**", min_value=2018, max_value=2022)
                Quarter = st.slider("Quarter", min_value=1, max_value=4)
                
            with colum2:
                st.info(
                """
                #### From this menu we can get insights like :
                - Overall ranking on a particular Year and Quarter.
                - Top 10 State, District, Pincode based on Total number of transaction and Total amount spent on phonepe.
                - Top 10 State, District, Pincode based on Total phonepe users and their app opening frequency.
                - Top 10 mobile brands and its percentage based on the how many people use phonepe.
                """,icon="🔍"
                )
                
            # Top Charts - TRANSACTIONS    
            if Type == "Transactions":
                col1, col2 = st.columns([1, 1], gap="small")

                with col1:
                    st.markdown("<h1 style='" + garamond_bold_style + "'>State</h1>", unsafe_allow_html=True)
                    df = data['File 5']  
                    df = df[(df['Year'] == Year) & (df['Quarter'] == Quarter)]
                    df = df.groupby('State').agg({'Transaction_count': 'sum', 'Transaction_amount': 'sum'}).reset_index()
                    df = df.nlargest(10, 'Transaction_amount')
                    fig = px.pie(df, values='Transaction_amount', names='State', title='Top 10 States',
                                color_discrete_sequence=px.colors.sequential.Agsunset, hover_data=['Transaction_count'],
                                labels={'Transaction_count': 'Transactions_Count'})

                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(height=500, width=600) 
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.markdown("<p style='" + garamond_bold_style2 + "'>According to the latest data, Karnataka and Telangana have emerged as the leading states in India for PhonePe usage, accounting for the years 2018 to 2022, as depicted in the pie chart above.</h1>", unsafe_allow_html=True)

                with col2:
                    st.markdown("<h1 style='" + garamond_bold_style + "'>District</h1>", unsafe_allow_html=True)
                    df = data['File 3'] 
                    df = df[(df['Year'] == Year) & (df['Quarter'] == Quarter)]
                    df = df.groupby('District').agg({'Count': 'sum', 'Amount': 'sum'}).reset_index()
                    df.Count = df.Count.astype(float)
                    df = df.nlargest(10, 'Count')  # Select only the top 10 districts
                    fig = px.pie(df, values='Amount', names='District', title='Top 10 Districts',
                                color_discrete_sequence=px.colors.sequential.Agsunset, hover_data=['Count'],
                                labels={'Count': 'Transactions_Count'})

                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(height=500, width=600)  
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.markdown("<h1 style='" + garamond_bold_style2 + "'>Bengaluru Urban district is a hotspot for PhonePe payments, with a significant share of all PhonePe transactions in India, as evidenced in the pie chart..</h1>", unsafe_allow_html=True)
                            
            if Type == "Users":
                
                col1, col2 = st.columns([2, 2], gap="small")
            
                with col1:
                    st.markdown("<h1 style='" + garamond_bold_style + "'>Brands</h1>", unsafe_allow_html=True)
                    if Year == 2022 and Quarter in [2, 3, 4]:
                        st.markdown("#### Sorry No Data to Display for 2022 Qtr 2,3,4</h1>", unsafe_allow_html=True)
                    else:
                        df = data['File 2']  
                        df = df[(df['Year'] == Year) & (df['Quarter'] == Quarter)]
                        df = df.groupby('Brands').agg({'Count': 'sum', 'Percentage': 'mean'}).reset_index()
                        df['Avg_Percentage'] = df['Percentage'] * 100
                        df = df.nlargest(10, 'Count')
                        fig = px.bar(df, title='Top 10 Brands',
                                    x="Count", y="Brands", orientation='h', color='Avg_Percentage',
                                    color_continuous_scale=px.colors.sequential.Agsunset)
                        fig.update_layout(height=600, width=800)  
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.markdown("<p style='" + garamond_bold_style2 + "'> It was found that Xiaomi brand users are among the highest users of PhonePe in India, with million transactions, as visualized in the above bar chart..</h1>", unsafe_allow_html=True)
                        
                with col2:
                    st.markdown("<h1 style='" + garamond_bold_style + "'>District</h1>", unsafe_allow_html=True)
                    if Year == 2022 and Quarter in [2, 3, 4]:
                        st.markdown("#### Sorry No Data to Display for 2022 Qtr 2,3,4</h1>", unsafe_allow_html=True)
                    else:
                        df = data['File 4'] 
                        df = df[(df['Year'] == Year) & (df['Quarter'] == Quarter)]
                        df = df.groupby('District').agg({'RegisteredUser': 'sum', 'AppOpens': 'sum'}).reset_index()
                        df.RegisteredUser = df.RegisteredUser.astype(float)
                        df = df.nlargest(10, 'RegisteredUser')  # Select only the top 10 districts
                        fig = px.bar(df, title='Top 10 Districts',
                                    x="RegisteredUser", y="District", orientation='h', color='RegisteredUser',
                                    color_continuous_scale=px.colors.sequential.Agsunset)
                        fig.update_layout(height=600, width=800) 
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.markdown("<h1 style='" + garamond_bold_style2 + "'>In Bengaluru Urban district,  million users are actively using PhonePe for their transactions, making it the top choice for digital payments in the region, as demonstrated in the above bar chart..</h1>", unsafe_allow_html=True)
                        
                col3, col4 = st.columns([2, 2], gap="small")   

                with col3:
                    st.markdown("<h1 style='" + garamond_bold_style + "'>Pincode</h1>", unsafe_allow_html=True)
                    if Year == 2022 and Quarter in [2, 3, 4]:
                        st.markdown("#### Sorry No Data to Display for 2022 Qtr 2,3,4</h1>", unsafe_allow_html=True)
                    else:
                        df = data['File 6'] 
                        df = df[(df['Year'] == Year) & (df['Quarter'] == Quarter)]
                        df = df.groupby('Pincode').agg({'RegisteredUsers': 'sum'}).reset_index()
                        df = df.nlargest(10, 'RegisteredUsers')  # Select only the top 10 pincodes based on RegisteredUsers
                        fig = px.bar(df, x='Pincode', y='RegisteredUsers', title='Top 10 Pincodes',
                                    color='RegisteredUsers', color_continuous_scale=px.colors.sequential.Agsunset)
                        fig.update_layout(height=600, width=800) 
                        st.plotly_chart(fig, use_container_width=True)

                        st.markdown("<h1 style='" + garamond_bold_style2 + "'>In the bar chart above, it's evident that Pincode 201301 in Uttar Pradesh stands out as the highest user of PhonePe for transactions, with a significant volume of usage..</h1>", unsafe_allow_html=True)
                        
                    
                with col4:
                    st.markdown("<h1 style='" + garamond_bold_style + "'>State</h1>", unsafe_allow_html=True)
                    if Year == 2022 and Quarter in [2, 3, 4]:
                        st.markdown("#### Sorry No Data to Display for 2022 Qtr 2,3,4</h1>", unsafe_allow_html=True)
                    else:
                        df = data['File 4']
                        df = df[(df['Year'] == Year) & (df['Quarter'] == Quarter)]
                        df_state = df.groupby('State').agg({'RegisteredUser': 'sum', 'AppOpens': 'sum'}).reset_index()
                        df_state = df_state.nlargest(10, 'RegisteredUser')  # Select only the top 10 states based on RegisteredUser
                        fig = px.bar(df_state, x='State', y='RegisteredUser', title='Top 10 States',
                                    color='AppOpens', color_continuous_scale=px.colors.sequential.Agsunset)
                        fig.update_layout(height=600, width=800) 
                        st.plotly_chart(fig, use_container_width=True)
                    
                        st.markdown("<h1 style='" + garamond_bold_style2 + "'>Open apps are most popular in Maharashtra, where they see the highest level of usage compared to other states..</h1>", unsafe_allow_html=True)
            
        # Explore Data
        if selected == "Explore Data":
            st.markdown("<h1 style='" + garamond_bold_style + "'>Explore Data</h1>", unsafe_allow_html=True)
            File = st.sidebar.selectbox("**Select File**", ("Map Transactions", "Map Users"))
            colum1, colum2 = st.columns([1, 1.5], gap="large")

            with colum1:
                Year = st.slider("**Year**", min_value=2018, max_value=2022)
                Quarter = st.slider("Quarter", min_value=1, max_value=4)

            with colum2:
                st.info(
                    """
                    #### From this menu we can get insights like :
                    - Distribution of transactions or users across different regions using maps.
                    - Additional data exploration options.
                    """,
                    icon="🌐",
                )

            # Load data based on user selection
            if File == "Map Transactions":
                df_file3_map = data['File 3']
                st.write("<h1 style='" + garamond_bold_style2 + "'>Loaded data from Map Transactions.</h1>", unsafe_allow_html=True)

                #Print the shape of the DataFrame
                shape_info = f"<h1 style='{garamond_bold_style2}'>DataFrame Shape: {df_file3_map.shape}</h1>"
                st.write(shape_info, unsafe_allow_html=True)

                #Print the values of Year and Quarter
                year_info = f"<p style='{garamond_bold_style2}'>Selected Year: {Year}</p>"
                quarter_info = f"<p style='{garamond_bold_style2}'>Selected Quarter: {Quarter}</p>"

                st.write(year_info, unsafe_allow_html=True)
                st.write(quarter_info, unsafe_allow_html=True)


                # Filter data based on user selection
                filtered_data = df_file3_map[(df_file3_map["Year"] == Year) & (df_file3_map["Quarter"] == Quarter)]

                # Create a density heatmap using Plotly Express
                heatmap_fig = px.density_heatmap(
                    filtered_data, 
                    x='Quarter',  
                    y='State',    
                    z='Amount',  
                    color_continuous_scale="Agsunset"
                    
                )

                # Display the density heatmap in Streamlit
                st.plotly_chart(heatmap_fig, use_container_width=True)
                                
                
            elif File == "Map Users":
                df_file4_map = data['File 4']  # Load data from the appropriate CSV file
                st.write("<h1 style='" + garamond_bold_style2 + "'>Loaded data from Map Users.</h1>", unsafe_allow_html=True)

                #Print the shape of the DataFrame
                shape_info = f"<h1 style='{garamond_bold_style2}'>DataFrame Shape: {df_file4_map.shape}</h1>"
                st.write(shape_info, unsafe_allow_html=True)

                #Print the values of Year and Quarter
                year_info = f"<p style='{garamond_bold_style2}'>Selected Year: {Year}</p>"
                quarter_info = f"<p style='{garamond_bold_style2}'>Selected Quarter: {Quarter}</p>"

                st.write(year_info, unsafe_allow_html=True)
                st.write(quarter_info, unsafe_allow_html=True)

                # Filter data based on user selection
                df_file4_map = df_file4_map[(df_file4_map['Year'] == Year) & (df_file4_map['Quarter'] == Quarter)]

                
                # Create a treemap
                fig = px.treemap(
                    df_file4_map,
                    path=['State', 'District'],  
                    values='RegisteredUser',  
                    color='RegisteredUser',  
                    color_continuous_scale='Agsunset',  
                    title='Treemap of Users',
                    
                    # The size of the treemap
                    width=1000,  
                    height=2000,  
                                    
                    
                )

                st.plotly_chart(fig, use_container_width=True)

                    
        
        
        elif selected == "About":
            
            st.markdown("<h1 style='" + garamond_bold_style + "'>About PhonePe Pulse Data Visualization</h1>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>The PhonePe Pulse Data Visualization Dashboard is a powerful tool designed to help you explore and gain insights into digital transactions and user behavior on the PhonePe platform. It offers a user-friendly interface with interactive charts and visualizations to make data analysis easy and informative.</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True) 
            
            st.markdown("<h2 style='" + garamond_bold_style + "'>Purpose of the App</h2>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>The primary purpose of this app is to provide a comprehensive view of PhonePe's digital pulse. Whether you're interested in understanding transaction trends, top-performing regions, or user behavior, this app can help you explore the data efficiently.</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True) 

            st.markdown("<h3 style='" + garamond_bold_style + "'>App Description</h3>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>📊 This app provides insights into transaction data and user distribution across regions.</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)
            

            st.markdown("<h3 style='" + garamond_bold_style + "'>Features</h3>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>🔍 Visualize transaction and user data on maps.</p>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>📅 Explore data by selecting specific years and quarters.</p>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>🗺️ Gain insights into regional distribution of transactions and users.</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)

            st.markdown("<h3 style='" + garamond_bold_style + "'>How To Use</h3>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>1. Use the sidebar to select 'Explore Data'.</p>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>2. Choose the file type ('Map Transactions' or 'Map Users').</p>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>3. Adjust the year and quarter sliders to filter data.</p>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>4. View the map visualization and explore insights.</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)

            st.markdown("<h3 style='" + garamond_bold_style + "'>Privacy Policy</h3>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>🔒 Your privacy is important to us. We do not collect or store any personal data through this app.</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)

            st.markdown("<h3 style='" + garamond_bold_style + "'>Contact Us</h3>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>📧 If you have any questions, suggestions, or feedback, feel free to contact us at shaviskrit@gmail.com.</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)

            st.markdown("<h3 style='" + garamond_bold_style + "'>Data Sources</h3>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>📂 The data used in this app extracted from various sources, including aggregated transaction and user data, providing a comprehensive view of PhonePe's digital ecosystem.</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)

            st.markdown("<h3 style='" + garamond_bold_style + "'>Acknowledgments</h3>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>🙏 We would like to express our gratitude to the data providers and contributors who made this app possible.</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)

            st.markdown("<h3 style='" + garamond_bold_style + "'>Future Updates</h3>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>🚀 We are committed to enhancing this app with more features and functionalities in future updates.</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)

            st.markdown("<h3 style='" + garamond_bold_style + "'>Credits and Thanks </h3>", unsafe_allow_html=True)
            st.markdown("<p style='" + garamond_bold_style2 + "'>🙌 Thank you for using our app. We appreciate your support!</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)
            
            st.markdown("<h3 style='" + garamond_bold_style + "'>Version</h3>", unsafe_allow_html=True)        
            st.markdown("<p style='" + garamond_bold_style2 + "'>PhonePe Pulse Data Visualization App v1.0</p>", unsafe_allow_html=True)
            st.markdown("<hr>", unsafe_allow_html=True)
            
            
if __name__ == "__main__":
    main()

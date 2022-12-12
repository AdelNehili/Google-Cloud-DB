######## Program Begins Here ########

#### Import Python libraries ####
import sqlite3
from google.cloud import storage
import pandas as pd
import numpy as np
from datetime import datetime
import mysql.connector
import sys

#### details library ####
from requirement.connection_requirement import *

"""
print("On test : %d"%(1))
#### Establish Connetion ####
cnx = mysql.connector.connect(user = current_user, password=current_password, host=current_host, database=current_database)
                              
print("On test : %d"%(2))
cursor = cnx.cursor()


#### Connetion Established ####

#### Execute query ####
query1 = ("select * from my_first_table")

#query1 = ("show databases")
cursor.execute(query1)

#### Create dataframe from resultant table ####
frame = pd.DataFrame(cursor.fetchall())

frame.head()

#### Put columns names for the dataframe ####

frame.columns = [['user_index', 'user_name']]
frame.head()
"""

connection = None
try:
	connection = mysql.connector.connect(user = current_user, password=current_password, host=current_host, database=current_database)
	if connection.is_connected():
		db_Info = connection.get_server_info()
		print("Connected to MySQL Server version ", db_Info)
		cursor = connection.cursor()
		cursor.execute("select database();")
		record = cursor.fetchone()
		print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
"""
#### Inserting new records into the SQL table ####

new_cust = pd.read_csv("customers2.csv")

#### Inserting new records into the SQL table through For Loop ####

temp = 0
for i in range(len(new_cust)):
    customer_id = str(new_cust['customer_id'].iloc[i])
    age = int(new_cust['age'].iloc[i])
    gender = str(new_cust['gender'].iloc[i])
    home_airport = int(new_cust['home_airport'].iloc[i])
    ticket_num = str(new_cust['ticket_num'].iloc[i])
    passport_num = str(new_cust['passport_num'].iloc[i])
    first_name = str(new_cust['first_name'].iloc[i])
    last_name = str(new_cust['last_name'].iloc[i])
    email_addr = str(new_cust['email'].iloc[i])
    cust_profile = str(new_cust['cust_profile'].iloc[i])
    tel = str(new_cust['tel'].iloc[i])
    
    query2 = ("INSERT INTO customer (customer_id, age, gender, home_airport, ticket_num, passport_num, first_name,"
              " last_name, email_addr, tel, cust_profile)"
              "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (customer_id, age, gender, home_airport, ticket_num, passport_num, first_name, last_name, email_addr, tel, 
           cust_profile)
    cursor.execute(query2, val)
    cnx.commit()
    temp = temp + 1
    print(temp, "Record Inserted for ",first_name )

"""

#### Extremely Important: Close your SQL connection ####

cursor.close()
cnx.close()

######## Program Ends Here ########

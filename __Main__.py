import mysql.connector
from mysql.connector import Error
import pandas as pd

#Create server connection and return connection object
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

#Creates database using the cursor method
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

#Creates database connection on MySQL
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

#Takes in SQL query and executes on connected server 
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")

pw = "JrSwann22"
# Create the connection to MySQL server
connection = create_server_connection("localhost", "root", pw)

create_employee_table = """
CREATE TABLE IF NOT EXISTS employee (
  employee_id INT PRIMARY KEY,
  first_name VARCHAR(40) NOT NULL,
  last_name VARCHAR(40) NOT NULL,
  employment_status VARCHAR(40), 
  activity_suspicion VARCHAR(40),
  shift_start TIME,
  shift_end TIME
  );
 """

create_database_query = "CREATE DATABASE IF NOT EXISTS Employee_Events"
create_database(connection, create_database_query)
connection = create_db_connection("localhost", "root", pw, "Employee_Events") # Connect to the Database
execute_query(connection, create_employee_table) #Execute our defined query

employee_list = """
INSERT INTO employee VALUES
(1, 'William', 'Swann', 'Not Employed', 'Flagged', '09:00:00', '17:00:00'),
(2, 'Omar', 'White-Evans', 'Employed', 'Not Flagged', '09:00:00', '17:00:00')
"""
connection = create_db_connection("localhost", "root", pw, "Employee_Events")
execute_query(connection, employee_list)

q1 = """
SELECT *
FROM employee;
"""

connection = create_db_connection("localhost", "root", pw, "Employee_Events")
results = read_query(connection, q1)

for result in results:
  print(result)
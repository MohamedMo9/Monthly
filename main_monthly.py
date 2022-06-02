import snowflake.connector
import pandas as pd
import csv
import numpy as np

#########################################################################

with open('query_monthly.csv') as f:
    lines = f.readlines()

user_email = "mohamed.mohamed@transferwise.com"
conn = snowflake.connector.connect(user=user_email,
                                   account='rt27428.eu-central-1',
                                   authenticator="externalbrowser",
                                   database='ANALYTICS_DB',  # optional
                                   schema='REPORTS',  # optional
                                   warehouse='ANALYSTS',  # optional
                                   autocommit=True)  # optional


########################## TOTAL Calls #################################
def singular(header_name, col, quer):
    query = lines[quer]
    crsr = conn.cursor()
    query_output = crsr.execute(query, timeout=1500)
    this_week = query_output.fetchall()
    col_names = [h[0] for h in crsr.description]
    # Convert to Array
    arr = np.asarray(this_week)
    f = open('exported_monthly.csv', 'r')
    reader = csv.reader(f)
    mylist = list(reader)
    f.close()

    # Shift the insert location to the right (column 6)
    start_col = col
    try:
        for i in range(0, len(arr)):
            for x in range(0, len(arr[0])):
                mylist[0][start_col + x] = header_name
                mylist[i + 2][start_col + x] = arr[i][x]
                if i < 1:
                    mylist[i + 1][start_col + x] = col_names[x]

    except IndexError:
        pass
        # saving the work
    my_new_list = open('exported_monthly.csv', 'w', newline='')
    csv_writer = csv.writer(my_new_list)
    csv_writer.writerows(mylist)
    f.close()
    my_new_list.close()


############################################################################
def multiple(header_name, col, quer):
    query = lines[quer]
    crsr = conn.cursor()
    query_output = crsr.execute(query, timeout=1500)
    this_week = query_output.fetchall()
    col_names = [h[0] for h in crsr.description]
    # Convert to Array
    arr = np.asarray(this_week)
    f = open('exported_monthly.csv', 'r')
    reader = csv.reader(f)
    mylist = list(reader)
    f.close()
    # Shift the insert location to the right (column 6)
    start_col = col
    try:
        for i in range(0, len(arr)):
            for x in range(0, len(arr[0])):
                mylist[0][start_col + x] = header_name
                mylist[i + 2][start_col + x] = arr[i][x]
                if i < 1:
                    mylist[i + 1][start_col + x] = col_names[x]

    except IndexError:
        pass
    # saving the work
    my_new_list = open('exported_monthly.csv', 'w', newline='')
    csv_writer = csv.writer(my_new_list)
    csv_writer.writerows(mylist)
    f.close()
    my_new_list.close()



try:
    multiple("Phones", 0, 0)
except Exception as e:
    pass

print("Phones data done")
########################################################################
try:
    multiple("Chats", 10, 1)
except Exception as e:
    pass

print("Chats data done")
########################################################################
try:
    multiple("Chat AHT", 16, 2)
except Exception as e:
    pass

print("Chat AHT data done")
########################################################################
try:
    multiple("Emails", 22, 3)
except Exception as e:
    pass

print("Emails data done")
########################################################################
try:
    multiple("Adherence", 28, 4)
except Exception as e:
    pass

print("Adherence data done")
########################################################################
try:
    multiple("Emails AHT", 34, 5)
except Exception as e:
    pass

print("Emails AHT done")
########################################################################
try:
    multiple("CPH Phone", 40, 6)
except Exception as e:
    pass

print("CPH Phone done")
########################################################################
try:
    multiple("CPH Email", 46, 7)
except Exception as e:
    pass

print("CPH Email done")

########################################################################
try:
    multiple("CPH Chat", 52, 8)
except Exception as e:
    pass

print("CPH Chat done")
########################################################################
try:
    multiple("Chat Concurrency", 58, 9)
except Exception as e:
    pass

print("Chat concurrency done")


print("Data is ready, please import the report to google sheets")
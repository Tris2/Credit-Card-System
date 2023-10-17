#Req 3.1


import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import secrets
# Define your database connection details
db = mysql.connector.connect(
    host = "localhost",
    user = secrets.mysql_username,
    passwd = secrets.mysql_password,
    database = "creditcard_capstone" #Database in mysql
)
# SQL query to find transaction type with the highest transaction count
query = """
SELECT TRANSACTION_TYPE, COUNT(*) AS transaction_count
FROM CDW_SAPP_CREDIT_CARD
GROUP BY TRANSACTION_TYPE
ORDER BY transaction_count DESC
LIMIT 1;
"""
# Execute the query and retrieve data
cursor = db.cursor()
cursor.execute(query)
result = cursor.fetchone()
# Extract transaction type and count
transaction_type = result[0]
transaction_count = result[1]
# Plot the result
plt.figure(figsize=(8, 6))
plt.bar(transaction_type, transaction_count)
plt.xlabel("Transaction Type")
plt.ylabel("Transaction Count")
plt.title("Transaction Type with the Highest Transaction Count")
plt.tight_layout()
# Save the visualization to a folder in your GitHub repository with a proper name
plt.savefig("path_to_github_repo/transaction_type_highest_count.png")
plt.show()
# Close the database connection
cursor.close()
db.close()
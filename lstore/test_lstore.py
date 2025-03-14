from lstore.db import Database
from lstore.query import Query

# Initialize database
db = Database()
db.open("test_db")  # Database folder will be created

# Create table
table = db.create_table("Students", 5, 0)  # 5 columns, primary key at index 0
query = Query(table)

# Insert records
print("\n=== INSERT RECORDS ===")
query.insert(1, 100, 200, 300, 400)
query.insert(2, 150, 250, 350, 450)
query.insert(3, 175, 275, 375, 475)

# Select and print results
print("\n=== SELECT RECORD ===")
records = query.select(2, 0, [1, 1, 1, 1, 1])  # Search key: 2, Project all columns

if records is False:
    print("❌ No record found for search key 2.")
else:
    for rec in records:
        print(f"Selected Record: {rec.columns}")


# Update a record
print("\n=== UPDATE RECORD ===")
query.update(2, None, 999, None, None, None)  # Update column 1 (index 1) to 999
updated_records = query.select(2, 0, [1, 1, 1, 1, 1])
for rec in updated_records:
    print(f"Updated Record: {rec.columns}")

# Delete a record
print("\n=== DELETE RECORD ===")
query.delete(2)
deleted_records = query.select(2, 0, [1, 1, 1, 1, 1])
print(f"Deleted Record Exists? {'Yes' if deleted_records else 'No'}")

# Sum query
print("\n=== SUM QUERY ===")
result = query.sum(1, 3, 2)  # Sum column index 2 over primary key range 1-3
print(f"Sum of column 2 (PK 1-3): {result}")

# Close and reopen to test persistence
print("\n=== TEST PERSISTENCE ===")
db.close()
db2 = Database()
db2.open("test_db")  # Load from disk
table2 = db2.get_table("Students")
query2 = Query(table2)
persisted_records = query2.select(1, 0, [1, 1, 1, 1, 1])
for rec in persisted_records:
    print(f"Persisted Record: {rec.columns}")

db2.close()

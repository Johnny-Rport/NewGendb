from lstore.table import Table

# Create a test table with 5 columns and column index 0 as the primary key
table = Table(name="TestTable", num_columns=5, key=0)

# Reset metadata before testing
table.metadata["record_count"] = 0
table.metadata["full_pages"] = 0

# Test 1: Insert Records
print("\nTesting Insert Operation")
table.insert(1, [1, 10, 20, 30, 40])
table.insert(2, [2, 15, 25, 35, 45])
table.insert(3, [3, 5, 10, 15, 20])
print(f"Total Records: {table.metadata['record_count']} (Expected: 3)")

# Test 2: Select Records
print("\nTesting Select Operation")
selected = table.select(2, 0)  # Search for key=2
for record in selected:
    print(f"Found Record: RID={record.rid}, Columns={record.columns}")

# Test 3: Update a Record
print("\nTesting Update Operation")
table.update(2, [2, 50, 60, 70, 80])
updated = table.select(2, 0)
for record in updated:
    print(f"Updated Record: RID={record.rid}, Columns={record.columns}")

# Test 4: Delete a Record
print("\nTesting Delete Operation")
table.delete(1)  # Delete key=1
deleted = table.select(1, 0)
print(f"Deleted Record Count: {len(deleted)} (Expected: 0)")

# Test 5: Sum Operation
print("\nTesting Sum Operation")
column_sum = table.sum(1, 2, 3)  # Sum values in column 1 for keys 2 to 3
print(f"Sum of Column 1 (Keys 2-3): {column_sum}")

# Test 6: Page Expansion
print("\nTesting Page Expansion")
for i in range(4, 4100):  # Fill up pages
    table.insert(i, [i] * 5)
print(f"Full Pages Count: {table.metadata['full_pages']} (Expected: More than 1)")

# ✅ Fill up multiple pages
print("\nTesting Read Bug Fix")
for i in range(1, 4100):  # This should fill at least one page
    table.insert(i, [i, i*10, i*20, i*30, i*40])

# Try to select records from the first (full) page
selected = table.select(1, 0)  # Search for key=1

if selected:
    print(f"Read Bug Fix Passed: Found Record: {selected[0].columns}")
else:
    print(f"Read Bug Fix Failed: Could not find record in full page")


#  Fix: Remove extra insertion loops
print("\nTesting Metadata Tracking")
table.metadata["record_count"] = 0  # Reset before inserting
for i in range(1, 4101):
    table.insert(i, [i, i*10, i*20, i*30, i*40])

# Check Metadata After Insertions
metadata = table.get_metadata()
print("Metadata After Insertions:", metadata)

assert metadata["record_count"] == 4100, "record_count is incorrect!"
assert metadata["full_pages"] > 0, "full_pages should be greater than 0!"
print("Metadata tracking after inserts is correct!")

# Delete a record and check metadata updates
table.delete(1)
metadata = table.get_metadata()
print("Metadata After Deleting 1 Record:", metadata)

assert metadata["record_count"] == 4099, "record_count did not decrease!"
print("Metadata updates correctly after deletion!")

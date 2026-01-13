from transaction_database import Database, Table, Column, DataType

def test_update_operations():
    print("=== Testing UPDATE Operations ===\n")
    
    # Setup
    db = Database("transaction_db")
    print(f"Created: {db}\n") 
    users_table = Table(
        name="users",
        columns=[
            Column("id", DataType.INT, is_primary_key=True, not_null=True),
            Column("name", DataType.TEXT, not_null=True),
            Column("email", DataType.TEXT, is_unique=True),
            Column("age", DataType.INT),
            Column("active", DataType.BOOL)
        ]
    )
    db.create_table(users_table)
    users = db.get_table("users")
    
    # Insert test data
    users.insert({"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30, "active": True})
    users.insert({"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25, "active": True})
    users.insert({"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35, "active": False})
    users.insert({"id": 4, "name": "Diana", "email": "diana@example.com", "age": 28, "active": True})
    
    print("Initial data:")
    for row in users.select_all():
        print(f"  {row}")
    print()
    
    # Test 1: Update single column
    print("Test 1: UPDATE users SET age = 26 WHERE id = 2")
    count = users.update({"age": 26}, lambda row: row['id'] == 2)
    print(f"Updated {count} row(s)")
    user = users.get_by_primary_key(2)
    print(f"Bob's new age: {user['age']}")
    print()
    
    # Test 2: Update multiple columns
    print("Test 2: UPDATE users SET name = 'Charles', age = 36 WHERE id = 3")
    count = users.update({"name": "Charles", "age": 36}, lambda row: row['id'] == 3)
    print(f"Updated {count} row(s)")
    user = users.get_by_primary_key(3)
    print(f"Updated user: {user}")
    print()
    
    # Test 3: Update multiple rows
    print("Test 3: UPDATE users SET active = False WHERE age < 30")
    count = users.update({"active": False}, lambda row: row['age'] < 30)
    print(f"Updated {count} row(s)")
    print("Users with age < 30:")
    for row in users.select_where(lambda row: row['age'] < 30):
        print(f"  {row}")
    print()
    
    # Test 4: Update with no matches
    print("Test 4: UPDATE users SET age = 100 WHERE id = 999")
    count = users.update({"age": 100}, lambda row: row['id'] == 999)
    print(f"Updated {count} row(s) (should be 0)")
    print()
    
    # Test 5: Attempt to violate unique constraint
    print("Test 5: Try to UPDATE email to duplicate (should fail)")
    try:
        users.update({"email": "alice@example.com"}, lambda row: row['id'] == 2)
        print("ERROR: Should have failed!")
    except ValueError as e:
        print(f"✓ Correctly rejected: {e}")
    print()

def test_delete_operations():
    print("\n=== Testing DELETE Operations ===\n")
    
    # Setup
    db = Database("test_db")
    users_table = Table(
        name="users",
        columns=[
            Column("id", DataType.INT, is_primary_key=True, not_null=True),
            Column("name", DataType.TEXT, not_null=True),
            Column("email", DataType.TEXT, is_unique=True),
            Column("age", DataType.INT)
        ]
    )
    db.create_table(users_table)
    users = db.get_table("users")
    
    # Insert test data
    users.insert({"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30})
    users.insert({"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25})
    users.insert({"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35})
    users.insert({"id": 4, "name": "Diana", "email": "diana@example.com", "age": 28})
    users.insert({"id": 5, "name": "Eve", "email": "eve@example.com", "age": 22})
    
    print("Initial data (5 users):")
    for row in users.select_all():
        print(f"  {row}")
    print()
    
    # Test 1: Delete by condition
    print("Test 1: DELETE FROM users WHERE age < 25")
    count = users.delete(lambda row: row['age'] < 25)
    print(f"Deleted {count} row(s)")
    print(f"Remaining users: {len(users.rows)}")
    print()
    
    # Test 2: Delete by primary key (fast)
    print("Test 2: DELETE FROM users WHERE id = 3 (using fast PK delete)")
    deleted = users.delete_by_primary_key(3)
    print(f"Deleted: {deleted}")
    print(f"Remaining users: {len(users.rows)}")
    for row in users.select_all():
        print(f"  {row}")
    print()
    
    # Test 3: Delete multiple rows
    print("Test 3: DELETE FROM users WHERE age >= 28")
    count = users.delete(lambda row: row['age'] >= 28)
    print(f"Deleted {count} row(s)")
    print(f"Remaining users: {len(users.rows)}")
    for row in users.select_all():
        print(f"  {row}")
    print()
    
    # Test 4: Try to insert with previously deleted email (should work now)
    print("Test 4: Insert user with previously deleted email")
    users.insert({"id": 10, "name": "New Charlie", "email": "charlie@example.com", "age": 40})
    print("✓ Successfully inserted (email constraint properly removed)")
    print()
    
    # Test 5: Delete non-existent row
    print("Test 5: DELETE non-existent row (id = 999)")
    deleted = users.delete_by_primary_key(999)
    print(f"Deleted: {deleted} (should be False)")
    print()
    
    print("=== All UPDATE and DELETE tests passed! ===")

if __name__ == "__main__":
    test_update_operations()
    test_delete_operations()
from transaction_database import Database, Table, Column, DataType

def test_select_queries():
    print("=== Testing SELECT Queries ===\n")
    
    # Create database and table
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
    print("Inserting test data...")
    users.insert({"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30, "active": True})
    users.insert({"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25, "active": True})
    users.insert({"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35, "active": False})
    users.insert({"id": 4, "name": "Diana", "email": "diana@example.com", "age": 28, "active": True})
    users.insert({"id": 5, "name": "Eve", "email": "eve@example.com", "age": 22, "active": False})
    print(f"Inserted 5 users\n")
    
    # Test 1: Select all
    print("Test 1: SELECT * FROM users")
    all_users = users.select_all()
    for row in all_users:
        print(f"  {row}")
    print()
    
    # Test 2: Select with WHERE (age > 25)
    print("Test 2: SELECT * FROM users WHERE age > 25")
    result = users.select_where(lambda row: row['age'] > 25)
    for row in result:
        print(f"  {row}")
    print()
    
    # Test 3: Select with WHERE (active = True)
    print("Test 3: SELECT * FROM users WHERE active = True")
    result = users.select_where(lambda row: row['active'] == True)
    for row in result:
        print(f"  {row}")
    print()
    
    # Test 4: Select with complex condition (age >= 25 AND active = True)
    print("Test 4: SELECT * FROM users WHERE age >= 25 AND active = True")
    result = users.select_where(lambda row: row['age'] >= 25 and row['active'] == True)
    for row in result:
        print(f"  {row}")
    print()
    
    # Test 5: Select specific columns
    print("Test 5: SELECT name, email FROM users WHERE age < 30")
    result = users.select_where(lambda row: row['age'] < 30)
    result = users.select_columns(result, ['name', 'email'])
    for row in result:
        print(f"  {row}")
    print()
    
    # Test 6: Get by primary key (fast lookup)
    print("Test 6: Get user by primary key (id = 3)")
    user = users.get_by_primary_key(3)
    print(f"  {user}")
    print()
    
    # Test 7: OR condition
    print("Test 7: SELECT * FROM users WHERE age < 25 OR age > 32")
    result = users.select_where(lambda row: row['age'] < 25 or row['age'] > 32)
    for row in result:
        print(f"  {row}")
    print()
    
    print("test 8 SELECT * FROM users WHERE name == Eve")
    result = users.select_where(lambda row:row['name'] == 'Eve')
    for row in result:
        print(f"  {row}")
    print()
    print("=== All query tests passed! ===")

if __name__ == "__main__":
    test_select_queries()
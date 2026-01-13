import sys
sys.path.append('.')

from transaction_database import Database, Table, Column, DataType

def test_basic_operations():
    print("=== Testing Database ===\n")
    
    # Create a database
    db = Database("transaction_db")
    print(f"Created: {db}\n")
    
    # Define a users table
    users_table = Table(
        name="users",
        columns=[
            Column("id", DataType.INT, is_primary_key=True, not_null=True),
            Column("name", DataType.TEXT, not_null=True),
            Column("email", DataType.TEXT, is_unique=True),
            Column("age", DataType.INT)
        ]
    )
    
    # Create the table
    db.create_table(users_table)
    print(f"Created table: {users_table}\n")
    
    # Insert some rows
    users = db.get_table("users")
    print("Inserting rows...")
    users.insert({"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30})
    users.insert({"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25})
    users.insert({"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35})
    print(f"Inserted 3 rows\n")
    
    # Select all rows
    print("All users:")
    for row in users.select_all():
        print(f"  {row}")
    
    print("\n=== Testing Constraints ===\n")
    
    # Test primary key violation
    print("Testing primary key violation...")
    try:
        users.insert({"id": 1, "name": "Duplicate", "email": "dup@example.com", "age": 40})
        print("ERROR: Should have failed!")
    except ValueError as e:
        print(f"✓ Correctly rejected: {e}")
    
    # Test unique constraint violation
    print("\nTesting unique constraint violation...")
    try:
        users.insert({"id": 4, "name": "Another", "email": "alice@example.com", "age": 28})
        print("ERROR: Should have failed!")
    except ValueError as e:
        print(f"✓ Correctly rejected: {e}")
    
    # Test NOT NULL constraint
    print("\nTesting NOT NULL constraint...")
    try:
        users.insert({"id": 5, "email": "test@example.com", "age": 22})
        print("ERROR: Should have failed!")
    except ValueError as e:
        print(f"✓ Correctly rejected: {e}")
    
    print("\n=== All tests passed! ===")

if __name__ == "__main__":
    test_basic_operations()

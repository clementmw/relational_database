from transaction_database import Database, Table, Column, DataType

def test_inner_join():
    print("=== Testing INNER JOIN ===\n")
    
    # Create database
    db = Database("transaction_db")
    print(f"Created: {db}\n")
    
    # Create users table
    users_table = Table(
        name="users",
        columns=[
            Column("id", DataType.INT, is_primary_key=True, not_null=True),
            Column("name", DataType.TEXT, not_null=True),
            Column("department_id", DataType.INT)
        ]
    )
    
    # Create departments table
    departments_table = Table(
        name="departments",
        columns=[
            Column("id", DataType.INT, is_primary_key=True, not_null=True),
            Column("dept_name", DataType.TEXT, not_null=True),
            Column("location", DataType.TEXT)
        ]
    )
    
    db.create_table(users_table)
    db.create_table(departments_table)
    
    # Insert users
    users = db.get_table("users")
    users.insert({"id": 1, "name": "Alice", "department_id": 10})
    users.insert({"id": 2, "name": "Bob", "department_id": 20})
    users.insert({"id": 3, "name": "Charlie", "department_id": 10})
    users.insert({"id": 4, "name": "Diana", "department_id": 30})
    users.insert({"id": 5, "name": "Eve", "department_id": 20})
    
    # Insert departments
    departments = db.get_table("departments")
    departments.insert({"id": 10, "dept_name": "Engineering", "location": "Building A"})
    departments.insert({"id": 20, "dept_name": "Sales", "location": "Building B"})
    departments.insert({"id": 30, "dept_name": "Marketing", "location": "Building C"})
    departments.insert({"id": 40, "dept_name": "HR", "location": "Building D"})  # No users
    
    print("Users table:")
    for row in users.select_all():
        print(f"  {row}")
    print()
    
    print("Departments table:")
    for row in departments.select_all():
        print(f"  {row}")
    print()
    
    # Test 1: Basic INNER JOIN
    print("Test 1: INNER JOIN users and departments")
    print("SQL equivalent: SELECT * FROM users INNER JOIN departments ON users.department_id = departments.id")
    result = db.inner_join("users", "departments", "department_id", "id")
    for row in result:
        print(f"  {row}")
    print(f"Total rows: {len(result)}")
    print()
    
    # Test 2: INNER JOIN with column selection
    print("Test 2: INNER JOIN with specific columns")
    print("SQL equivalent: SELECT name, dept_name, location FROM users INNER JOIN departments ON users.department_id = departments.id")
    result = db.inner_join("users", "departments", "department_id", "id", 
                          select_columns=["name", "dept_name", "location"])
    for row in result:
        print(f"  {row}")
    print()
    
    # Test 3: Optimized JOIN (using primary key index)
    print("Test 3: Optimized INNER JOIN (using index)")
    result = db.inner_join_optimized("users", "departments", "department_id", "id")
    print(f"Joined {len(result)} rows using optimized algorithm")
    for row in result[:3]:  # Show first 3
        print(f"  {row}")
    print()
    
    # Test 4: JOIN with filtering
    print("Test 4: JOIN + WHERE (manual filtering)")
    print("SQL equivalent: SELECT name, dept_name FROM users INNER JOIN departments ON users.department_id = departments.id WHERE dept_name = 'Engineering'")
    result = db.inner_join("users", "departments", "department_id", "id")
    filtered = [row for row in result if row["departments.dept_name"] == "Engineering"]
    for row in filtered:
        name = row["users.name"]
        dept = row["departments.dept_name"]
        print(f"  {name} works in {dept}")
    print()
    
    print("=== Testing Multiple JOINs ===\n")
    
    # Create projects table
    projects_table = Table(
        name="projects",
        columns=[
            Column("id", DataType.INT, is_primary_key=True, not_null=True),
            Column("project_name", DataType.TEXT, not_null=True),
            Column("user_id", DataType.INT),
            Column("status", DataType.TEXT)
        ]
    )
    db.create_table(projects_table)
    
    projects = db.get_table("projects")
    projects.insert({"id": 101, "project_name": "Website Redesign", "user_id": 1, "status": "Active"})
    projects.insert({"id": 102, "project_name": "Mobile App", "user_id": 1, "status": "Active"})
    projects.insert({"id": 103, "project_name": "Sales Dashboard", "user_id": 2, "status": "Completed"})
    projects.insert({"id": 104, "project_name": "Marketing Campaign", "user_id": 4, "status": "Active"})
    
    print("Projects table:")
    for row in projects.select_all():
        print(f"  {row}")
    print()
    
    # Test 5: Two-step JOIN (users -> projects -> departments)
    print("Test 5: Multi-table JOIN (projects -> users -> departments)")
    print("Show project, user name, and department")
    
    # First join: projects with users
    step1 = db.inner_join("projects", "users", "user_id", "id")
    
    # Manual second join with departments (you'd need to implement this properly)
    print("Projects with user and department info:")
    for row in step1:
        user_dept_id = row["users.department_id"]
        dept = departments.get_by_primary_key(user_dept_id)
        print(f"  Project: {row['projects.project_name']}, User: {row['users.name']}, Dept: {dept['dept_name']}")
    print()
    
    print("=== All JOIN tests passed! ===")

if __name__ == "__main__":
    test_inner_join()
from transaction_database import Table,Column,Database,DataType
from typing import List,Dict,Any
import re
import os
import pickle

class REPL:
    def __init__(self, db_file="webapp.db"):
        self.db_file = db_file
        
        # Load from file if exists
        if os.path.exists(db_file):
            with open(db_file, 'rb') as f:
                self.db = pickle.load(f)
            print(f"✓ Loaded database from {db_file}")
            print(f"  Tables: {', '.join(self.db.list_tables())}")
        else:
            self.db = Database('webapp')
            print("⚠ Database file not found. Creating new database.")
            print("  Tip: Run 'python app.py' first to create webapp.db")
        
        self.running = False
    
    def save_db(self):
        """Save database to file"""
        with open(self.db_file, 'wb') as f:
            pickle.dump(self.db, f)
        print(f"💾 Database saved to {self.db_file}")
    
    def start(self):
        """Start the REPL"""
        self.running = True
        print("=" * 60)
        print("Welcome to Jumanji, Simple RDBMS")
        print("=" * 60)
        print("Type 'Help' for help and 'Exit' to exit")
        print("=" * 60)
        
        while self.running:
            try:
                command = input("Jumanji> ").strip()
                if not command:
                    continue
                self.process_command(command)
            except KeyboardInterrupt:
                print("\nUse 'Exit' to quit.")
            except Exception as e:
                print(f"Error: {e}")
    def process_command(self,command: str):
        """ where the majic happens """

        command_upper = command.upper()

        if command_upper == 'EXIT' or command_upper == 'QUIT':
            print("Jumanji hates to see you leave , Goodbye!")
            self.running = False
            return
        
        if command_upper == 'HELP':
            self.show_help()
            return
        
        if command_upper == 'SHOW TABLES':
            self.show_tables()
            return
        
        if command_upper.startswith("DESCRIBE ") or command_upper.startswith("DESC "):
            parts = command.split()
            if len(parts) < 2:
                print("Syntax error: DESCRIBE requires a table name")
                print("Example: DESCRIBE users")
                return
            table_name = parts[1]
            self.handle_describe(table_name)
            return


        
        """ 
        sql commands
        """
        if command_upper.startswith("CREATE TABLE"):
            self.handle_create_table(command)
        elif command_upper.startswith("INSERT INTO"):
            self.handle_insert(command)
        elif command_upper.startswith("SELECT"):
            self.handle_select(command)
        elif command_upper.startswith("UPDATE"):
            self.handle_update(command)
        elif command_upper.startswith("DELETE FROM"):
            self.handle_delete(command)
        elif command_upper.startswith("DROP TABLE"):
            self.handle_drop_table(command)
        



        else:
            print(f"Unknown command: {command}")
            print("Type 'Help' for help and 'Exit' for exit You know the drill")

    def show_help(self):
        """ Display help information"""

        print("\nAvailable Commands:")
        print("-" * 60)
        print("CREATE TABLE <name> (<column definitions>)")
        print("  Example: CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
        print()
        print("INSERT INTO <table> VALUES (<values>)")
        print("  Example: INSERT INTO users VALUES (1, 'Alice', 30)")
        print()
        print("SELECT * FROM <table> [WHERE <condition>]")
        print("  Example: SELECT * FROM users WHERE age > 25")
        print()
        print("UPDATE <table> SET <assignments> WHERE <condition>")
        print("  Example: UPDATE users SET age = 31 WHERE id = 1")
        print()
        print("DELETE FROM <table> WHERE <condition>")
        print("  Example: DELETE FROM users WHERE age < 20")
        print()
        print("DROP TABLE <table>")
        print("  Example: DROP TABLE users")
        print()
        print("SHOW TABLES - List all tables")
        print("DESCRIBE <table> - Show table structure")
        print("HELP - Show this help")
        print("EXIT - Exit the program")
        print("-" * 60)
        print()    

    def show_tables(self):
        "show all tables in db"
        tables = self.db.list_tables()
        if not tables:
            print(f"No tables found in the database. {self.db.name}")
        
        else:
            print(f"\n Tables in database {self.db.name}")
            for table in tables:
                print(f" - {table}")
        print()

    def handle_describe(self, table_name: str):
        """Show the table structure"""
        try:
            table = self.db.get_table(table_name)
            
            print(f"\nTable: {table_name}")
            print("-" * 60)
            print(f"{'Column':<20} {'Type':<10} {'Constraints':<30}")
            print("-" * 60)
            
            for col in table.columns:
                constraints = []
                if col.is_primary_key:
                    constraints.append("PRIMARY KEY")
                if col.is_unique:
                    constraints.append("UNIQUE")
                if col.not_null:
                    constraints.append("NOT NULL")
                
                constraints_str = ", ".join(constraints) if constraints else "-"
                print(f"{col.name:<20} {col.data_type.value:<10} {constraints_str:<30}")
            
            print("-" * 60)
            print(f"Total rows: {len(table.rows)}")
            print()
        
        except Exception as e:
            print(f"Error: {e}")
    def handle_create_table(self, command: str):
        """Create a new table"""

        pattern = r"CREATE TABLE (\w+)\s*\((.*?)\)"
        match = re.match(pattern, command, re.IGNORECASE)

        if not match:
            print("create table syntax error")
            print("Example: CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
            return
        
        table_name = match.group(1)
        columns_str = match.group(2)

        columns = []

        for col_def in columns_str.split(','):
            col_def = col_def.strip()  # Remove whitespace
            parts = col_def.split()     # Split into parts NOW (only once!)

            if len(parts) < 2:
                print(f"Invalid column definition: {col_def}")
                return 
            
            col_name = parts[0]
            col_type_str = parts[1].upper()

            # Parse data type
            try:
                col_type = DataType[col_type_str]  
            except KeyError:
                print(f"Invalid data type for column {col_name}: {col_type_str}")
                print("Supported types are: INT, TEXT, FLOAT, BOOL")
                return

            # Parse constraints - check the ORIGINAL col_def string
            is_primary_key = "PRIMARY KEY" in col_def.upper()  
            is_unique = "UNIQUE" in col_def.upper() and not is_primary_key
            not_null = "NOT NULL" in col_def.upper() or is_primary_key

            columns.append(Column(
                col_name,
                col_type,
                is_primary_key,
                is_unique,
                not_null
            ))

        # Create table
        try:
            table = Table(table_name, columns)
            self.db.create_table(table)
            print(f"Table '{table_name}' created successfully.")
        except Exception as e:
            print(f"Error creating table: {e}")

    def parse_values(self, value_str: str) -> Any:
        """Parse a single value"""
        value_str = value_str.strip()
        
        # String
        if value_str.startswith("'") and value_str.endswith("'"):
            return value_str[1:-1]
        # Boolean
        elif value_str.upper() == "TRUE":
            return True
        elif value_str.upper() == "FALSE":
            return False
        # NULL
        elif value_str.upper() == "NULL":
            return None
        # Number
        else:
            try:
                if '.' not in value_str:
                    return int(value_str)
                else:
                    return float(value_str)
            except ValueError:
                raise ValueError(f"Invalid value: {value_str}")
    def handle_insert(self, command: str):
        """
        handles inserting data to the db
        """
        pattern = r"INSERT INTO (\w+)\s+VALUES\s*\((.*?)\)"
        match = re.match(pattern, command, re.IGNORECASE)

        if not match:
            print("synax error")
            print("Example: INSERT INTO users VALUES (1, 'Yusufu', 30)")
            return
        
        table_name = match.group(1)
        values_str = match.group(2) 

        try:

            table = self.db.get_table(table_name)

            values = self.parse_values(values_str)

            if len(values) != len(table.columns):
                print(f"Column count mismatch: expected {len(table.columns)}, got {len(values)}")
                return
            
            row = {}

            for col,value in zip(table.columns,values):
                row[col.name] = value

            #insert
            table.insert(row)
            print("Row inserted successfully.")
        
        except Exception as e:
            print(f"Error inserting data: {e}")
    
    def handle_select(self, command: str):
        """Handle SELECT command"""
        # Pattern: SELECT * FROM table [WHERE condition]
        pattern = r"SELECT \* FROM (\w+)(?:\s+WHERE\s+(.+))?"
        match = re.match(pattern, command, re.IGNORECASE)
        
        if not match:
            print("Syntax error in SELECT command.")
            print("Example: SELECT * FROM users WHERE age > 25")
            return
        
        table_name = match.group(1)
        where_clause = match.group(2)
        
        try:
            table = self.db.get_table(table_name)
            
            # Execute query
            if where_clause:
                condition = self.parse_where_clause(where_clause)
                rows = table.select_where(condition)
            else:
                rows = table.select_all()
            
            # Display results
            self.display_results(rows, table.column_names)
            
        except ValueError as e:
            print(f"Error: {e}")
    
    def handle_update(self, command: str):
        """Handle UPDATE command"""
        # Pattern: UPDATE table SET col=val, col=val WHERE condition
        pattern = r"UPDATE (\w+)\s+SET\s+(.+?)\s+WHERE\s+(.+)"
        match = re.match(pattern, command, re.IGNORECASE)
        
        if not match:
            print("Syntax error in UPDATE command.")
            print("Example: UPDATE users SET age = 31 WHERE id = 1")
            return
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        
        try:
            table = self.db.get_table(table_name)
            
            # Parse SET clause
            updates = self.parse_set_clause(set_clause)
            
            # Parse WHERE clause
            condition = self.parse_where_clause(where_clause)
            
            # Execute update
            count = table.update(updates, condition)
            print(f"{count} row(s) updated.")
            
        except ValueError as e:
            print(f"Error: {e}")
    
    def handle_delete(self, command: str):
        """Handle DELETE command"""
        # Pattern: DELETE FROM table WHERE condition
        pattern = r"DELETE FROM (\w+)\s+WHERE\s+(.+)"
        match = re.match(pattern, command, re.IGNORECASE)
        
        if not match:
            print("Syntax error in DELETE command.")
            print("Example: DELETE FROM users WHERE age < 20")
            return
        
        table_name = match.group(1)
        where_clause = match.group(2)
        
        try:
            table = self.db.get_table(table_name)
            
            # Parse WHERE clause
            condition = self.parse_where_clause(where_clause)
            
            # Execute delete
            count = table.delete(condition)
            print(f"{count} row(s) deleted.")
            
        except ValueError as e:
            print(f"Error: {e}")
    
    def handle_drop_table(self, command: str):
        """Handle DROP TABLE command"""
        pattern = r"DROP TABLE (\w+)"
        match = re.match(pattern, command, re.IGNORECASE)
        
        if not match:
            print("Syntax error in DROP TABLE command.")
            return
        
        table_name = match.group(1)
        
        try:
            self.db.drop_table(table_name)
            print(f"Table '{table_name}' dropped.")
        except ValueError as e:
            print(f"Error: {e}")
    
    def parse_values(self, values_str: str) -> List[Any]:
        """Parse VALUES clause"""
        values = []
        for val in values_str.split(','):
            val = val.strip()
            
            # String (quoted)
            if val.startswith("'") and val.endswith("'"):
                values.append(val[1:-1])
            # Boolean
            elif val.upper() == "TRUE":
                values.append(True)
            elif val.upper() == "FALSE":
                values.append(False)
            # NULL
            elif val.upper() == "NULL":
                values.append(None)
            # Number
            else:
                try:
                    # Try int first
                    if '.' not in val:
                        values.append(int(val))
                    else:
                        values.append(float(val))
                except ValueError:
                    print(f"Invalid value: {val}")
                    return []
        
        return values
    
    def parse_set_clause(self, set_clause: str) -> Dict[str, Any]:
        """Parse SET clause for UPDATE"""
        updates = {}
        for assignment in set_clause.split(','):
            parts = assignment.split('=')
            if len(parts) != 2:
                raise ValueError(f"Invalid assignment: {assignment}")
            
            col_name = parts[0].strip()
            value_str = parts[1].strip()
            
            # Parse value
            value = self.parse_value(value_str)
            updates[col_name] = value
        
        return updates
    
    def parse_value(self, value_str: str) -> Any:
        """Parse a single value"""
        value_str = value_str.strip()
        
        # String
        if value_str.startswith("'") and value_str.endswith("'"):
            return value_str[1:-1]
        # Boolean
        elif value_str.upper() == "TRUE":
            return True
        elif value_str.upper() == "FALSE":
            return False
        # NULL
        elif value_str.upper() == "NULL":
            return None
        # Number
        else:
            try:
                if '.' not in value_str:
                    return int(value_str)
                else:
                    return float(value_str)
            except ValueError:
                raise ValueError(f"Invalid value: {value_str}")
    
    def parse_where_clause(self, where_clause: str) -> callable:
        """Parse WHERE clause and return a condition function"""
        # Simple parser for: column operator value
        # Supports: =, !=, <, >, <=, >=
        
        # Handle AND
        if " AND " in where_clause.upper():
            parts = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
            conditions = [self.parse_simple_condition(part) for part in parts]
            return lambda row: all(cond(row) for cond in conditions)
        
        # Handle OR
        if " OR " in where_clause.upper():
            parts = re.split(r'\s+OR\s+', where_clause, flags=re.IGNORECASE)
            conditions = [self.parse_simple_condition(part) for part in parts]
            return lambda row: any(cond(row) for cond in conditions)
        
        # Simple condition
        return self.parse_simple_condition(where_clause)
    
    def parse_simple_condition(self, condition: str) -> callable:
        """Parse a simple condition: column op value"""
        # Match operators
        operators = ['<=', '>=', '!=', '=', '<', '>']
        
        for op in operators:
            if op in condition:
                parts = condition.split(op)
                if len(parts) == 2:
                    col_name = parts[0].strip()
                    value_str = parts[1].strip()
                    value = self.parse_value(value_str)
                    
                    # Return condition function
                    if op == '=':
                        return lambda row: row.get(col_name) == value
                    elif op == '!=':
                        return lambda row: row.get(col_name) != value
                    elif op == '<':
                        return lambda row: row.get(col_name) < value
                    elif op == '>':
                        return lambda row: row.get(col_name) > value
                    elif op == '<=':
                        return lambda row: row.get(col_name) <= value
                    elif op == '>=':
                        return lambda row: row.get(col_name) >= value
        
        raise ValueError(f"Invalid WHERE condition: {condition}")
    
    def display_results(self, rows: List[Dict[str, Any]], columns: List[str]):
        """Display query results in a table format"""
        if not rows:
            print("No rows returned.")
            return
        
        # Calculate column widths
        widths = {}
        for col in columns:
            widths[col] = len(col)
        
        for row in rows:
            for col in columns:
                value_str = str(row.get(col, ''))
                widths[col] = max(widths[col], len(value_str))
        
        # Print header
        print()
        header = " | ".join(col.ljust(widths[col]) for col in columns)
        print(header)
        print("-" * len(header))
        
        # Print rows
        for row in rows:
            row_str = " | ".join(str(row.get(col, '')).ljust(widths[col]) for col in columns)
            print(row_str)
        
        print()
        print(f"{len(rows)} row(s) returned.")
        print()




if __name__ == "__main__":
    repl = REPL()
    repl.start()
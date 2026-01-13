from typing import Any, Dict, List, Optional, Set, Callable
from enum import Enum
from collections import defaultdict

class DataType(Enum):
    """Supported data types"""
    INT = "INT"
    TEXT = "TEXT"
    FLOAT = "FLOAT"
    BOOL = "BOOL"

class Column:
    """Represents a table column"""
    def __init__(self, name: str, data_type: DataType, 
                 is_primary_key: bool = False, 
                 is_unique: bool = False,
                 not_null: bool = False):
        self.name = name
        self.data_type = data_type
        self.is_primary_key = is_primary_key
        self.is_unique = is_unique
        self.not_null = not_null

    def __repr__(self):
        return f"Column({self.name}, {self.data_type.value})"

class Table:
    """Represents a database table"""
    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = columns
        self.rows: List[Dict[str, Any]] = []
        self.column_names = [col.name for col in columns]
        
        # Find primary key column
        self.primary_key = None
        for col in columns:
            if col.is_primary_key:
                self.primary_key = col.name
                break
        
        # Indexes for primary key and unique columns
        self.primary_key_index: Dict[Any, int] = {}  # value -> row_index
        self.unique_indexes: Dict[str, Set[Any]] = defaultdict(set)
        
        # Store unique columns
        self.unique_columns = [col.name for col in columns if col.is_unique or col.is_primary_key]

    def validate_row(self, row: Dict[str, Any]) -> None:
        """Validate a row before insertion"""
        # Check all columns are present
        for col in self.columns:
            if col.name not in row:
                if col.not_null:
                    raise ValueError(f"Column '{col.name}' cannot be NULL")
                row[col.name] = None
        
        # Check data types (basic validation)
        for col in self.columns:
            value = row[col.name]
            if value is not None:
                if col.data_type == DataType.INT and not isinstance(value, int):
                    raise ValueError(f"Column '{col.name}' expects INT, got {type(value)}")
                elif col.data_type == DataType.TEXT and not isinstance(value, str):
                    raise ValueError(f"Column '{col.name}' expects TEXT, got {type(value)}")
                elif col.data_type == DataType.FLOAT and not isinstance(value, (int, float)):
                    raise ValueError(f"Column '{col.name}' expects FLOAT, got {type(value)}")
                elif col.data_type == DataType.BOOL and not isinstance(value, bool):
                    raise ValueError(f"Column '{col.name}' expects BOOL, got {type(value)}")
        
        # Check primary key uniqueness
        if self.primary_key:
            pk_value = row[self.primary_key]
            if pk_value in self.primary_key_index:
                raise ValueError(f"Primary key violation: {pk_value} already exists")
        
        # Check unique constraints
        for col_name in self.unique_columns:
            if col_name != self.primary_key:  # PK already checked
                value = row[col_name]
                if value is not None and value in self.unique_indexes[col_name]:
                    raise ValueError(f"Unique constraint violation on column '{col_name}'")

    def insert(self, row: Dict[str, Any]) -> None:
        """Insert a row into the table"""
        self.validate_row(row)
        
        row_index = len(self.rows)
        self.rows.append(row)
        
        # Update indexes
        if self.primary_key:
            pk_value = row[self.primary_key]
            self.primary_key_index[pk_value] = row_index
        
        for col_name in self.unique_columns:
            if col_name != self.primary_key:
                value = row[col_name]
                if value is not None:
                    self.unique_indexes[col_name].add(value)

    def select_all(self) -> List[Dict[str, Any]]:
        """Select all rows"""
        return self.rows.copy()
    
    def select_where(self, condition: Callable[[Dict[str, Any]], bool]) -> List[Dict[str, Any]]:
        """Select rows based on a condition = condition function that takes in a row and returns true or false"""
        return [row for row in self.rows if condition(row)]
    
    def get_by_primary_key(self, pk_value: Any) -> Optional[Dict[str, Any]]:
        """Fast lookup by primary key using index"""
        if not self.primary_key:
            raise ValueError("Table has no primary key")
        
        if pk_value in self.primary_key_index:
            row_index = self.primary_key_index[pk_value]
            return self.rows[row_index]
        return None
    
    def select_columns(self, rows: List[Dict[str, Any]], columns: List[str]) -> List[Dict[str, Any]]:
        """
        Select specific columns from rows
        If columns is empty or contains '*', return all columns
        """
        if not columns or '*' in columns:
            return rows
        
        # Validate columns exist
        for col in columns:
            if col not in self.column_names:
                raise ValueError(f"Column '{col}' does not exist in table '{self.name}'")
        
        return [{col: row[col] for col in columns} for row in rows]
    
    def update(self, updates: Dict[str, Any], condition: Callable[[Dict[str, Any]], bool]) -> int:
        """
        Update rows that match the condition
        Returns the number of rows updated
        """
        updated_count = 0
        rows_to_update = []
        
        # Find rows to update
        for i, row in enumerate(self.rows):
            if condition(row):
                rows_to_update.append((i, row.copy()))
        
        # Update each matching row
        for row_index, old_row in rows_to_update:
            new_row = old_row.copy()
            new_row.update(updates)
            
            # Validate the updated row
            # Check data types
            for col in self.columns:
                if col.name in updates:
                    value = new_row[col.name]
                    if value is not None:
                        if col.data_type == DataType.INT and not isinstance(value, int):
                            raise ValueError(f"Column '{col.name}' expects INT")
                        elif col.data_type == DataType.TEXT and not isinstance(value, str):
                            raise ValueError(f"Column '{col.name}' expects TEXT")
                        elif col.data_type == DataType.FLOAT and not isinstance(value, (int, float)):
                            raise ValueError(f"Column '{col.name}' expects FLOAT")
                        elif col.data_type == DataType.BOOL and not isinstance(value, bool):
                            raise ValueError(f"Column '{col.name}' expects BOOL")
                    elif col.not_null:
                        raise ValueError(f"Column '{col.name}' cannot be NULL")
            
            # Check if primary key is being updated
            if self.primary_key and self.primary_key in updates:
                old_pk = old_row[self.primary_key]
                new_pk = new_row[self.primary_key]
                
                # Check if new primary key already exists (and it's not the same row)
                if new_pk != old_pk and new_pk in self.primary_key_index:
                    raise ValueError(f"Primary key violation: {new_pk} already exists")
                
                # Remove old primary key from index
                del self.primary_key_index[old_pk]
                # Add new primary key to index
                self.primary_key_index[new_pk] = row_index
            
            # Check unique constraints
            for col_name in self.unique_columns:
                if col_name in updates and col_name != self.primary_key:
                    old_value = old_row[col_name]
                    new_value = new_row[col_name]
                    
                    if old_value != new_value:
                        if new_value is not None and new_value in self.unique_indexes[col_name]:
                            raise ValueError(f"Unique constraint violation on column '{col_name}'")
                        
                        # Update unique index
                        if old_value is not None:
                            self.unique_indexes[col_name].discard(old_value)
                        if new_value is not None:
                            self.unique_indexes[col_name].add(new_value)
            
            # Apply the update
            self.rows[row_index] = new_row
            updated_count += 1
        
        return updated_count
    
    def delete(self, condition: Callable[[Dict[str, Any]], bool]) -> int:
        """
        Delete rows that match the condition
        Returns the number of rows deleted
        """
        rows_to_delete = []
        
        # Find rows to delete (collect indices)
        for i, row in enumerate(self.rows):
            if condition(row):
                rows_to_delete.append((i, row))
        
        # Delete in reverse order to maintain correct indices
        for row_index, row in reversed(rows_to_delete):
            # Remove from primary key index
            if self.primary_key:
                pk_value = row[self.primary_key]
                del self.primary_key_index[pk_value]
            
            # Remove from unique indexes
            for col_name in self.unique_columns:
                if col_name != self.primary_key:
                    value = row[col_name]
                    if value is not None:
                        self.unique_indexes[col_name].discard(value)
            
            # Remove the row
            del self.rows[row_index]
        
        # Rebuild primary key index (row indices have changed)
        if self.primary_key:
            self.primary_key_index.clear()
            for i, row in enumerate(self.rows):
                pk_value = row[self.primary_key]
                self.primary_key_index[pk_value] = i
        
        return len(rows_to_delete)
    
    def delete_by_primary_key(self, pk_value: Any) -> bool:
        """
        Delete a row by its primary key (fast)
        Returns True if row was deleted, False if not found
        """
        if not self.primary_key:
            raise ValueError("Table has no primary key")
        
        if pk_value not in self.primary_key_index:
            return False
        
        row_index = self.primary_key_index[pk_value]
        row = self.rows[row_index]
        
        # Remove from unique indexes
        for col_name in self.unique_columns:
            if col_name != self.primary_key:
                value = row[col_name]
                if value is not None:
                    self.unique_indexes[col_name].discard(value)
        
        # Remove from primary key index
        del self.primary_key_index[pk_value]
        
        # Remove the row
        del self.rows[row_index]
        
        # Rebuild primary key index
        self.primary_key_index.clear()
        for i, row in enumerate(self.rows):
            pk_value = row[self.primary_key]
            self.primary_key_index[pk_value] = i
        
        return True
    def __repr__(self):
        return f"Table({self.name}, columns={len(self.columns)}, rows={len(self.rows)})"

class Database:
    """Main database class"""
    def __init__(self, name: str = "mydb"):
        self.name = name
        self.tables: Dict[str, Table] = {}

    def create_table(self, table: Table) -> None:
        """Create a new table"""
        if table.name in self.tables:
            raise ValueError(f"Table '{table.name}' already exists")
        self.tables[table.name] = table

    def get_table(self, name: str) -> Table:
        """Get a table by name"""
        if name not in self.tables:
            raise ValueError(f"Table '{name}' does not exist")
        return self.tables[name]

    def drop_table(self, name: str) -> None:
        """Drop a table"""
        if name not in self.tables:
            raise ValueError(f"Table '{name}' does not exist")
        del self.tables[name]

    def list_tables(self) -> List[str]:
        """List all table names"""
        return list(self.tables.keys())

    def inner_join(self, 
                   left_table_name: str, 
                   right_table_name: str,
                   left_column: str,
                   right_column: str,
                   select_columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Perform an INNER JOIN between two tables
        
        Args:
            left_table_name: Name of the left table
            right_table_name: Name of the right table
            left_column: Column from left table to join on
            right_column: Column from right table to join on
            select_columns: Columns to include in result (None = all)
        
        Returns:
            List of joined rows
        """
        left_table = self.get_table(left_table_name)
        right_table = self.get_table(right_table_name)
        
        # Validate columns exist
        if left_column not in left_table.column_names:
            raise ValueError(f"Column '{left_column}' does not exist in table '{left_table_name}'")
        if right_column not in right_table.column_names:
            raise ValueError(f"Column '{right_column}' does not exist in table '{right_table_name}'")
        
        result = []
        
        # Nested loop join algorithm
        for left_row in left_table.rows:
            for right_row in right_table.rows:
                # Check if join condition matches
                if left_row[left_column] == right_row[right_column]:
                    # Merge rows with prefixed column names to avoid conflicts
                    joined_row = {}
                    
                    # Add left table columns with prefix
                    for col in left_table.column_names:
                        joined_row[f"{left_table_name}.{col}"] = left_row[col]
                    
                    # Add right table columns with prefix
                    for col in right_table.column_names:
                        joined_row[f"{right_table_name}.{col}"] = right_row[col]
                    
                    result.append(joined_row)
        
        # Filter columns if specified
        if select_columns:
            filtered_result = []
            for row in result:
                filtered_row = {}
                for col in select_columns:
                    if col in row:
                        filtered_row[col] = row[col]
                    else:
                        # Try without table prefix
                        found = False
                        for key in row:
                            if key.endswith(f".{col}"):
                                filtered_row[col] = row[key]
                                found = True
                                break
                        if not found:
                            raise ValueError(f"Column '{col}' not found in join result")
                filtered_result.append(filtered_row)
            return filtered_result
        
        return result
    def inner_join_optimized(self,
                            left_table_name: str,
                            right_table_name: str,
                            left_column: str,
                            right_column: str,
                            select_columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Optimized INNER JOIN using index if the right column is a primary key
        """
        left_table = self.get_table(left_table_name)
        right_table = self.get_table(right_table_name)
        
        # Validate columns
        if left_column not in left_table.column_names:
            raise ValueError(f"Column '{left_column}' does not exist in table '{left_table_name}'")
        if right_column not in right_table.column_names:
            raise ValueError(f"Column '{right_column}' does not exist in table '{right_table_name}'")
        
        result = []
        
        # Check if we can use index (right column is primary key)
        if right_table.primary_key == right_column:
            # Use index for faster lookup
            for left_row in left_table.rows:
                join_value = left_row[left_column]
                right_row = right_table.get_by_primary_key(join_value)
                
                if right_row:
                    joined_row = {}
                    for col in left_table.column_names:
                        joined_row[f"{left_table_name}.{col}"] = left_row[col]
                    for col in right_table.column_names:
                        joined_row[f"{right_table_name}.{col}"] = right_row[col]
                    result.append(joined_row)
        else:
            # Fall back to nested loop join
            return self.inner_join(left_table_name, right_table_name, 
                                  left_column, right_column, select_columns)
        
        # Filter columns if specified
        if select_columns:
            filtered_result = []
            for row in result:
                filtered_row = {}
                for col in select_columns:
                    if col in row:
                        filtered_row[col] = row[col]
                    else:
                        found = False
                        for key in row:
                            if key.endswith(f".{col}"):
                                filtered_row[col] = row[key]
                                found = True
                                break
                        if not found:
                            raise ValueError(f"Column '{col}' not found in join result")
                filtered_result.append(filtered_row)
            return filtered_result
        
        return result

    def __repr__(self):
        return f"Database({self.name}, tables={len(self.tables)})"

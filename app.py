from flask import Flask, render_template, request, redirect, url_for, flash
from transaction_database import Table,Column,Database,DataType
import os
from datetime import datetime
import pickle
from flask import jsonify


app = Flask(__name__)

DB_FILE = "webapp.db"

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, 'rb') as f:
            return pickle.load(f)

    db = Database('fraud_detection')
    

    users_table = Table("users", [
        Column("id", DataType.INT, is_primary_key=True),
        Column("first_name", DataType.TEXT, not_null=True),
        Column("last_name", DataType.TEXT, not_null=True),
        Column("email", DataType.TEXT, not_null=True, is_unique=True),
        Column("created_at", DataType.TEXT, not_null=True),
    ])
    db.create_table(users_table)

    transactions_table = Table("transactions", [
        Column("id", DataType.INT, is_primary_key=True),
        Column("user_id", DataType.INT, not_null=True),
        Column("amount", DataType.FLOAT, not_null=True),
        Column("timestamp", DataType.TEXT, not_null=True),
        Column("is_fraud", DataType.BOOL, not_null=True),
    ])
    db.create_table(transactions_table)

    # insert sample data
    users = db.get_table("users")
    users.insert({"id": 1, "first_name": "james", "last_name": "kamotho", "email": "kamothojames@example.com", "created_at": "2026-01-15"})
    users.insert({"id": 2, "first_name": "mary", "last_name": "wambui", "email": "marywambo@example.com","created_at":"2026-01-15"})

    transactions = db.get_table("transactions")
    transactions.insert({"id": 1, "user_id": 1, "amount": 100.0, "timestamp":"2026-01-15", "is_fraud": False})
    transactions.insert({"id": 2, "user_id": 2, "amount": 2500.0, "timestamp": "2026-01-15", "is_fraud": True})

    save_database(db)
    return db


def save_database(db):
    with open(DB_FILE, 'wb') as f:
        pickle.dump(db, f)


# load database
db = load_db()

@app.route('/')
def index():
    users = db.get_table("users")
    transactions = db.get_table("transactions")


    total_users = len(users.rows)
    total_transactions = len(transactions.rows)
    total_fraudulent = len([t for t in transactions.rows if t['is_fraud']])
    total_amount = sum([t['amount'] for t in transactions.rows])


    stat = {
        "total_users": total_users,
        "total_transactions": total_transactions,
        "fraud_count": total_fraudulent,
        "total_amount": total_amount,
    
    }
    return render_template('index.html', stats=stat)

@app.route('/users')
def users():
    users = db.get_table("users")
    all_users = users.select_all()
    return render_template('users.html', users=all_users)

@app.route('/add/user', methods=['POST'])
def add_user():
    try:
        users_table = db.get_table("users")
        get_id = max([u['id'] for u in users_table.rows],default=0)
        next_id = get_id + 1

        first_name = request.form['first_name']
        last_name = request.form['last_name']
        email = request.form['email']
        created_at = datetime.now().strftime("%Y-%m-%d")    

        users_table.insert({
            "id":next_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "created_at": created_at
        })
    
        save_database(db)
        return jsonify({"message": "User added successfully"}), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/users/delete/<int:user_id>', methods=['POST'])
def delete_user(user_id):
    """Delete a user"""
    try:
        users_table = db.get_table("users")
        deleted = users_table.delete_by_primary_key(user_id)
        
        if deleted:
            save_database(db)
            return jsonify({"success": True, "message": "User deleted"})
        else:
            return jsonify({"success": False, "message": "User not found"}), 404
    
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400

@app.route('/transactions')
def transactions():
    """List all transactions with user info (JOIN)"""
    try:
        # Get all users for the dropdown
        users_table = db.get_table("users")
        all_users = users_table.select_all()
        
        # Perform JOIN
        result = db.inner_join("transactions", "users", "user_id", "id")
        
        # Format for display
        formatted = []
        for row in result:
            formatted.append({
                'id': row['transactions.id'],
                'user_name': f"{row['users.first_name']} {row['users.last_name']}",
                'amount': row['transactions.amount'],
                'timestamp': row['transactions.timestamp'],
                'is_fraud': row['transactions.is_fraud']
            })
        
        return render_template('transactions.html', transactions=formatted, all_users=all_users)
    
    except Exception as e:
        users_table = db.get_table("users")
        all_users = users_table.select_all()
        return render_template('transactions.html', transactions=[], all_users=all_users, error=str(e))

@app.route('/transactions/add', methods=['POST'])
def add_transaction():
    """Add a new transaction"""
    try:
        transactions_table = db.get_table("transactions")
        
        # Get next ID
        max_id = max([t['id'] for t in transactions_table.rows], default=0)
        new_id = max_id + 1
        
        user_id = int(request.form['user_id'])
        amount = float(request.form['amount'])
        description = request.form['description']
        created_at = datetime.now().strftime("%Y-%m-%d")
        
        # Simple fraud detection: flag if amount > $1000
        is_fraud = amount >= 1000
        
        transactions_table.insert({
            "id": new_id,
            "user_id": user_id,
            "amount": amount,
            "description": description,
            "is_fraud": is_fraud,
            "timestamp": created_at
        })
        
        save_database(db)
        return jsonify({"success": True, "message": "Transaction added", "is_fraud": is_fraud})
    
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400

@app.route('/transactions/flag/<int:trans_id>', methods=['POST'])
def flag_transaction(trans_id):
    """Flag/unflag a transaction as fraud"""
    try:
        transactions_table = db.get_table("transactions")
        
        is_fraud = request.json.get('is_fraud', True)
        
        count = transactions_table.update(
            {"is_fraud": is_fraud},
            lambda row: row['id'] == trans_id
        )
        
        if count > 0:
            save_database(db)
            return jsonify({"success": True, "message": "Transaction updated"})
        else:
            return jsonify({"success": False, "message": "Transaction not found"}), 404
    
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics"""
    users = db.get_table("users")
    transactions = db.get_table("transactions")
    
    return jsonify({
        'total_users': len(users.rows),
        'total_transactions': len(transactions.rows),
        'fraud_count': len([t for t in transactions.rows if t['is_fraud']]),
        'total_amount': sum(t['amount'] for t in transactions.rows)
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
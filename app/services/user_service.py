import bcrypt
from pathlib import Path
from app.data.users import get_user_by_username, insert_user

def register_user(username, password, role='user'):
    # Check if user exists
    if get_user_by_username(username):
        return False, f"Username '{username}' already exists."
    
    # Hash password
    password_hash = bcrypt.hashpw(
        password.encode('utf-8'),
        bcrypt.gensalt()
    ).decode('utf-8')
    
    # Insert user
    insert_user(username, password_hash, role)
    return True, f"User '{username}' registered successfully."

def login_user(username, password):
    user = get_user_by_username(username)
    if not user:
        return False, "User not found."
    
    stored_hash = user[2]  # password_hash column
    if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
        return True, f"Welcome, {username}!"
    return False, "Incorrect password."
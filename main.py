# main.py — Complete Week 8 Implementation
from pathlib import Path
from app.data.db import connect_database
from app.data.schema import create_all_tables
from app.services.user_service import register_user, login_user
from app.data.incidents import insert_incident, get_all_incidents
import pandas as pd

# Create DATA folder
DATA_DIR = Path("DATA")
DATA_DIR.mkdir(exist_ok=True)

def load_all_csv_data(conn):
    """Load all CSV files into database tables."""
    csv_files = {
        'cyber_incidents.csv': 'cyber_incidents',
        'datasets_metadata.csv': 'datasets_metadata', 
        'it_tickets.csv': 'it_tickets'
    }
    
    total_rows = 0
    for csv_file, table_name in csv_files.items():
        csv_path = DATA_DIR / csv_file
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                df.to_sql(table_name, conn, if_exists='append', index=False)
                total_rows += len(df)
                print(f"✅ Loaded {len(df)} rows into {table_name}")
            except Exception as e:
                print(f"❌ Error loading {csv_file}: {e}")
        else:
            print(f"⚠️  CSV file not found: {csv_file}")
    
    return total_rows

def setup_database_complete():
    """Complete database setup with CSV loading."""
    print("\n" + "="*60)
    print("STARTING COMPLETE DATABASE SETUP")
    print("="*60)
    
    conn = connect_database()
    
    # Create tables
    create_all_tables(conn)
    
    # Load CSV data
    total_rows = load_all_csv_data(conn)
    print(f"📊 Loaded {total_rows} total records from CSV files")
    
    # Verify setup
    cursor = conn.cursor()
    tables = ['users', 'cyber_incidents', 'datasets_metadata', 'it_tickets']
    print("\n Database Summary:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"   {table}: {count} rows")
    
    conn.close()
    print("\n✅ DATABASE SETUP COMPLETE!")

def main():
    print("=" * 60)
    print("Week 8: Database Demo")
    print("=" * 60)
    
    # Run complete setup
    setup_database_complete()
    
    # Test authentication
    print("\n--- Testing Authentication ---")
    success, msg = register_user("alice", "SecurePass123!", "analyst")
    print(f"Register: {msg}")
    
    success, msg = login_user("alice", "SecurePass123!")
    print(f"Login: {msg}")
    
    # Test CRUD operations
    print("\n--- Testing CRUD Operations ---")
    incident_id = insert_incident(
        "2024-11-05",
        "Phishing",
        "High",
        "Open", 
        "Test incident",
        "alice"
    )
    print(f"Created incident #{incident_id}")
    
    # Query data
    df = get_all_incidents()
    print(f"Total incidents: {len(df)}")
    
    print("\n" + "=" * 60)
    print("✅ SETUP COMPLETED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    main()

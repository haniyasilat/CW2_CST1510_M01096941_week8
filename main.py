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
    """Load all CSV files into database tables with proper column mapping."""
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
                print(f"🔍 DEBUG - Columns in {csv_file}: {list(df.columns)}")
                print(f"🔍 DEBUG - First row: {df.iloc[0].to_dict()}")
                
                # Map CSV columns to database columns for each table
                if table_name == 'cyber_incidents':
                    # Create a new DataFrame with the expected database columns
                    mapped_data = []
                    for _, row in df.iterrows():
                        mapped_row = {
                            'date_reported': row.get('Date', '2024-01-01'),
                            'incident_type': row.get('Type', 'Unknown'),
                            'severity': 'Medium',  # Default since we don't have severity
                            'status': 'Open',      # Default
                            'description': f"{row.get('Title', '')} - {row.get('Description', '')}",
                            'reported_by': 'System'
                        }
                        mapped_data.append(mapped_row)
                    
                    df = pd.DataFrame(mapped_data)
                    print(f"🔍 Mapped cyber_incidents: {len(df)} rows")
                
                elif table_name == 'datasets_metadata':
                    # Map datasets metadata
                    mapped_data = []
                    for _, row in df.iterrows():
                        mapped_row = {
                            'dataset_name': row.get('dataset_name', 'Unknown Dataset'),
                            'source': row.get('source_organization', 'Unknown Source'),
                            'record_count': 0,  # Default
                            'last_updated': row.get('last_updated', '2024-01-01'),
                            'description': row.get('description', 'No description available')
                        }
                        mapped_data.append(mapped_row)
                    
                    df = pd.DataFrame(mapped_data)
                    print(f"🔍 Mapped datasets_metadata: {len(df)} rows")
                
                elif table_name == 'it_tickets':
                    # Map IT tickets
                    mapped_data = []
                    for i, row in df.iterrows():
                        mapped_row = {
                            'ticket_id': f"TICKET_{i+1000}",
                            'date_created': '2024-01-01',  # Default date
                            'priority': row.get('Category', 'Medium') if 'Category' in row else 'Medium',
                            'status': 'Open',
                            'description': row.get('Customer Input', 'No description'),
                            'assigned_to': 'Unassigned'
                        }
                        mapped_data.append(mapped_row)
                    
                    df = pd.DataFrame(mapped_data)
                    print(f"🔍 Mapped it_tickets: {len(df)} rows")
                
                # Load the mapped data
                if not df.empty:
                    df.to_sql(table_name, conn, if_exists='replace', index=False)
                    total_rows += len(df)
                    print(f"✅ Loaded {len(df)} rows into {table_name}")
                else:
                    print(f"⚠️  No data to load for {table_name}")
                
            except Exception as e:
                print(f"❌ Error loading {csv_file}: {e}")
                import traceback
                traceback.print_exc()
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
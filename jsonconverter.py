import psycopg2
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
DATABASE_URL = os.getenv('DATABASE_URL')  # e.g., "postgresql://user:password@localhost:5432/clinical_trials_db"
TABLE_NAME = "clinical_trials"  # Replace with your table name
OUTPUT_DIR = "json_files"  # Directory to save JSON files

def fetch_table_data():
    """Fetch all rows from the specified table."""
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Fetch all rows from the table
        cursor.execute(f"SELECT nct_id, data FROM {TABLE_NAME};")
        rows = cursor.fetchall()

        # Close the connection
        cursor.close()
        conn.close()

        return rows

    except Exception as e:
        print(f"Error fetching data from the database: {e}")
        return None

def save_json_files(rows):
    """Save the `data` column from each row as a JSON file."""
    try:
        # Create the output directory if it doesn't exist
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)

        # Iterate through each row
        for row in rows:
            nct_id, data = row

            # Define the JSON file name
            json_file = os.path.join(OUTPUT_DIR, f"{nct_id}.json")

            # Write the JSON data to the file
            with open(json_file, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=4)  # Pretty-print JSON with indentation

            print(f"Saved JSON file: {json_file}")

        print(f"All JSON files saved to {OUTPUT_DIR}")

    except Exception as e:
        print(f"Error saving JSON files: {e}")

def main():
    # Fetch data from the table
    rows = fetch_table_data()

    if rows:
        # Save the `data` column from each row as a JSON file
        save_json_files(rows)

if __name__ == "__main__":
    main()
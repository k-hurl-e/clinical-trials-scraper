import psycopg2
import csv
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database connection parameters
DATABASE_URL = os.getenv('DATABASE_URL')  # e.g., "postgresql://user:password@localhost:5432/clinical_trials_db"
TABLE_NAME = "clinical_trials"  # Replace with your table name
OUTPUT_CSV = "clinical_trials.csv"  # Output CSV file name

def fetch_table_data():
    """Fetch all rows from the specified table."""
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Fetch all rows from the table
        cursor.execute(f"SELECT * FROM {TABLE_NAME};")
        rows = cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Close the connection
        cursor.close()
        conn.close()

        return column_names, rows

    except Exception as e:
        print(f"Error fetching data from the database: {e}")
        return None, None

def save_to_csv(column_names, rows, output_file):
    """Save the fetched data to a CSV file."""
    try:
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)

            # Write the header (column names)
            writer.writerow(column_names)

            # Write the rows
            writer.writerows(rows)

        print(f"Data successfully saved to {output_file}")

    except Exception as e:
        print(f"Error saving data to CSV: {e}")

def main():
    # Fetch data from the table
    column_names, rows = fetch_table_data()

    if column_names and rows:
        # Save the data to a CSV file
        save_to_csv(column_names, rows, OUTPUT_CSV)

if __name__ == "__main__":
    main()
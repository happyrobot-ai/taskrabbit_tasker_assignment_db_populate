import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import logging
import argparse
from datetime import datetime
import pytz

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TaskerAssignmentDBPopulator:
    def __init__(self, use_test_tables=False):
        """Initialize the database populator with connection parameters."""
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT', 5432),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.engine = None
        self.use_test_tables = use_test_tables
        
        # Define table names based on test/prod mode
        if use_test_tables:
            self.tasks_table = 'test_taskrabbit_tasks_1'
            self.tasker_data_table = 'test_taskrabbit_tasker_data_1'
        else:
            self.tasks_table = 'taskrabbit_tasks_1'
            self.tasker_data_table = 'taskrabbit_tasker_data_1'
        
    def connect_to_database(self):
        """Establish connection to PostgreSQL database."""
        try:
            connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(connection_string)
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def read_csv_file(self, csv_path):
        """Read CSV file and return DataFrame with standardized column names."""
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Successfully read CSV file: {csv_path}")
            logger.info(f"CSV contains {len(df)} rows and {len(df.columns)} columns")
            
            # Map CSV column names to standardized names
            column_mapping = {
                'Tasker ID': 'tasker_id',
                'Name': 'name',
                'Email': 'email',
                'Phone Number': 'phone_number',
                'Tenure Months': 'tenure_months',
                'Lifetime Submitted Invoices Bucket': 'lifetime_submitted_invoices_bucket',
                'Metro Name': 'metro_name',
                'Job Id': 'job_id',
                'Postal Code': 'postal_code',
                'Latitude': 'latitude',
                'Longitude': 'longitude',
                'Country Key': 'country_key',
                'Latest Schedule Start At': 'latest_schedule_start_at',
                'Time Zone': 'time_zone',
                'Is Job Bundle': 'is_job_bundle',
                'Is Assigned': 'is_assigned',
                'Is Accepted': 'is_accepted',
                'Is Scheduled': 'is_scheduled',
                'Marketplace Key': 'marketplace_key',
                'Description': 'description',
                'Duration Hours': 'duration_hours',
                'Tasker Take Home Pay': 'tasker_take_home_pay'
            }
            
            # Rename columns to standardized names
            df = df.rename(columns=column_mapping)
            logger.info("Successfully standardized column names")
            
            return df
        except Exception as e:
            logger.error(f"Failed to read CSV file: {e}")
            return None
    
    def convert_timezone(self, df):
        """
        Convert latest_schedule_start_at to proper timezone based on time_zone field.
        """
        try:
            # Convert latest_schedule_start_at to datetime if it's not already
            df['latest_schedule_start_at'] = pd.to_datetime(df['latest_schedule_start_at'])
            
            # Create a function to convert timezone for each row
            def convert_row_timezone(row):
                try:
                    if pd.isna(row['latest_schedule_start_at']) or pd.isna(row['time_zone']):
                        return row['latest_schedule_start_at']
                    
                    # Get the timezone object
                    tz = pytz.timezone(row['time_zone'])
                    
                    # If the datetime is naive (no timezone info), assume it's UTC
                    if row['latest_schedule_start_at'].tzinfo is None:
                        # Assume the datetime is in UTC and localize it
                        utc_dt = pytz.UTC.localize(row['latest_schedule_start_at'])
                    else:
                        utc_dt = row['latest_schedule_start_at']
                    
                    # Convert to the target timezone
                    return utc_dt.astimezone(tz)
                    
                except Exception as e:
                    logger.warning(f"Timezone conversion failed for row: {e}")
                    return row['latest_schedule_start_at']
            
            # Apply timezone conversion
            df['latest_schedule_start_at'] = df.apply(convert_row_timezone, axis=1)
            
            logger.info("Successfully converted timestamps to proper timezones")
            return df
            
        except Exception as e:
            logger.error(f"Error converting timezones: {e}")
            return df
    
    def populate_tasks_table(self, df, replace_existing=False):
        """
        Populate the tasks table with job-related data from CSV.
        Maps CSV columns to taskrabbit_tasks table columns.
        """
        try:
            # Define the columns to extract for tasks table
            tasks_columns = [
                'tasker_id', 'metro_name', 'job_id', 'postal_code', 'latitude', 'longitude', 
                'country_key', 'latest_schedule_start_at', 'time_zone', 
                'is_job_bundle', 'is_assigned', 'is_accepted', 'is_scheduled', 
                'marketplace_key', 'description', 'duration_hours', 'tasker_take_home_pay'
            ]
            
            # Extract only the required columns
            tasks_df = df[tasks_columns].copy()
            
            # No need to remove duplicates - each job_id should be unique
            # The database will handle any primary key constraints
            
            # Insert data into tasks table
            if_exists_mode = 'replace' if replace_existing else 'append'
            tasks_df.to_sql(
                self.tasks_table, 
                self.engine, 
                if_exists=if_exists_mode, 
                index=False,
                method='multi'
            )
            
            logger.info(f"Successfully populated {self.tasks_table} with {len(tasks_df)} records")
            return True
            
        except Exception as e:
            logger.error(f"Error populating tasks table: {e}")
            return False
    
    def populate_tasker_data_table(self, df, replace_existing=False):
        """
        Populate the tasker data table with tasker-related data from CSV.
        Maps CSV columns to taskrabbit_tasker_data table columns.
        """
        try:
            # Define the columns to extract for tasker data table
            tasker_columns = [
                'tasker_id', 'name', 'email', 'phone_number', 
                'tenure_months', 'lifetime_submitted_invoices_bucket'
            ]
            
            # Extract only the required columns
            tasker_df = df[tasker_columns].copy()
            
            # Remove duplicates based on tasker_id (each tasker should appear only once)
            tasker_df = tasker_df.drop_duplicates(subset=['tasker_id'])
            
            # Insert data into tasker data table
            if_exists_mode = 'replace' if replace_existing else 'append'
            tasker_df.to_sql(
                self.tasker_data_table, 
                self.engine, 
                if_exists=if_exists_mode, 
                index=False,
                method='multi'
            )
            
            logger.info(f"Successfully populated {self.tasker_data_table} with {len(tasker_df)} records")
            return True
            
        except Exception as e:
            logger.error(f"Error populating tasker data table: {e}")
            return False
    
    def run_population(self, csv_path, replace_existing=False):
        """Main method to run the database population process."""
        logger.info("Starting database population process")
        logger.info(f"Using {'test' if self.use_test_tables else 'production'} tables")
        logger.info(f"Tasks table: {self.tasks_table}")
        logger.info(f"Tasker data table: {self.tasker_data_table}")
        logger.info(f"Mode: {'REPLACE existing data' if replace_existing else 'APPEND to existing data'}")
        
        # Connect to database
        if not self.connect_to_database():
            return False
        
        # Read CSV file
        df = self.read_csv_file(csv_path)
        if df is None:
            return False
        
        # Convert timezones
        df = self.convert_timezone(df)
        
        # Validate CSV columns
        required_columns = [
            'tasker_id', 'name', 'email', 'phone_number', 'tenure_months', 
            'lifetime_submitted_invoices_bucket', 'metro_name', 'job_id', 
            'postal_code', 'latitude', 'longitude', 'country_key', 
            'latest_schedule_start_at', 'time_zone', 'is_job_bundle', 
            'is_assigned', 'is_accepted', 'is_scheduled', 'marketplace_key', 
            'description', 'duration_hours', 'tasker_take_home_pay'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns in CSV: {missing_columns}")
            return False
        
        # Populate tables
        try:
            # Populate tasks table first
            if not self.populate_tasks_table(df, replace_existing):
                return False
            
            # Populate tasker data table
            if not self.populate_tasker_data_table(df, replace_existing):
                return False
            
            logger.info("Database population completed successfully")
            return True
        except Exception as e:
            logger.error(f"Error during table population: {e}")
            return False

def main():
    """Main function to run the database population with command line arguments."""
    parser = argparse.ArgumentParser(description='Populate TaskRabbit database tables from CSV')
    parser.add_argument('--csv-path', required=True, help='Path to the CSV file')
    parser.add_argument('--test', action='store_true', help='Use test tables instead of production tables')
    parser.add_argument('--replace', action='store_true', help='Replace existing data instead of appending')
    parser.add_argument('--env-file', help='Path to .env file (default: .env)')
    
    args = parser.parse_args()
    
    # Load environment variables from specified file
    if args.env_file:
        load_dotenv(args.env_file)
    else:
        load_dotenv()
    
    # Create populator instance
    populator = TaskerAssignmentDBPopulator(use_test_tables=args.test)
    
    # Run population
    success = populator.run_population(args.csv_path, replace_existing=args.replace)
    
    if success:
        print("✅ Database population completed successfully!")
    else:
        print("❌ Database population failed. Check logs for details.")
        exit(1)

if __name__ == "__main__":
    main()

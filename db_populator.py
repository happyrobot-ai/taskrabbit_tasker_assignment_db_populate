import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
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
                'Tasker Take Home Pay': 'tasker_take_home_pay',
                'Locale': 'locale',
                'Trimmed Address': 'trimmed_address'
            }
            
            # Rename columns to standardized names
            df = df.rename(columns=column_mapping)
            logger.info("Successfully standardized column names")
            
            # Fill empty duration_hours with default value of 2 hours
            if 'duration_hours' in df.columns:
                empty_count = df['duration_hours'].isna().sum() + (df['duration_hours'] == '').sum()
                df['duration_hours'] = df['duration_hours'].replace('', None)
                df['duration_hours'] = df['duration_hours'].fillna(2.0)
                if empty_count > 0:
                    logger.info(f"Filled {empty_count} empty duration_hours values with default of 2.0 hours")
            
            return df
        except Exception as e:
            logger.error(f"Failed to read CSV file: {e}")
            return None
    
    def convert_timezone(self, df):
        """
        Convert latest_schedule_start_at from UTC to the timezone specified in time_zone column.
        Assumes timestamps in CSV are in UTC. Converts to target timezone and stores as timezone-naive.
        """
        try:
            # Convert latest_schedule_start_at to datetime if it's not already
            df['latest_schedule_start_at'] = pd.to_datetime(df['latest_schedule_start_at'])
            
            # Check if time_zone column exists
            if 'time_zone' not in df.columns:
                logger.warning("time_zone column not found, skipping timezone conversion")
                # Remove timezone info if present to keep naive
                if df['latest_schedule_start_at'].dt.tz is not None:
                    df['latest_schedule_start_at'] = df['latest_schedule_start_at'].dt.tz_localize(None)
                return df
            
            # Get UTC timezone
            utc_tz = pytz.utc
            
            # Function to convert each row from UTC to target timezone
            def convert_utc_to_timezone(row):
                try:
                    dt = row['latest_schedule_start_at']
                    timezone_str = row['time_zone']
                    
                    # Skip if datetime or timezone is missing
                    if pd.isna(dt) or pd.isna(timezone_str) or timezone_str == '':
                        return dt
                    
                    # If datetime already has timezone, use it; otherwise assume UTC
                    if dt.tzinfo is None:
                        # Assume UTC and localize
                        utc_dt = utc_tz.localize(dt)
                    else:
                        # Convert to UTC first if it's in a different timezone
                        utc_dt = dt.astimezone(utc_tz)
                    
                    # Get target timezone
                    try:
                        target_tz = pytz.timezone(str(timezone_str))
                    except Exception:
                        logger.warning(f"Invalid timezone '{timezone_str}', keeping UTC")
                        target_tz = utc_tz
                    
                    # Convert from UTC to target timezone
                    converted_dt = utc_dt.astimezone(target_tz)
                    
                    # Remove timezone info to make it naive (for timezone-unaware database column)
                    return converted_dt.replace(tzinfo=None)
                    
                except Exception as e:
                    logger.warning(f"Timezone conversion failed for row: {e}")
                    # Return original datetime without timezone
                    if dt.tzinfo is not None:
                        return dt.replace(tzinfo=None)
                    return dt
            
            # Apply conversion row by row
            df['latest_schedule_start_at'] = df.apply(convert_utc_to_timezone, axis=1)
            
            # Log a sample to verify conversion
            sample_dt = df['latest_schedule_start_at'].iloc[0] if len(df) > 0 else None
            sample_tz = df['time_zone'].iloc[0] if len(df) > 0 and 'time_zone' in df.columns else None
            if sample_dt is not None and pd.notna(sample_dt):
                logger.info(f"Sample timestamp after conversion (timezone-naive): {sample_dt} (converted from UTC to {sample_tz})")
            
            logger.info("Successfully converted timestamps from UTC to specified timezones")
            return df
            
        except Exception as e:
            logger.error(f"Error processing timestamps: {e}")
            # Fallback: try to ensure datetime type is preserved
            try:
                df['latest_schedule_start_at'] = pd.to_datetime(df['latest_schedule_start_at'])
                if df['latest_schedule_start_at'].dt.tz is not None:
                    df['latest_schedule_start_at'] = df['latest_schedule_start_at'].dt.tz_localize(None)
            except Exception:
                pass
            return df

    def remove_apt_from_address(self, address):
        """Remove 'apt' from the address."""
        final_address = ''
        for word in address.split(' '):
            if word.lower() not in ['apt', 'unit', 'suite', 'building', 'floor', 'room', "apartment", "apt."]:
                final_address += word + ' '
            else:
                break
        final_address = final_address.strip()
        return final_address
    
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
                'marketplace_key', 'description', 'duration_hours', 'tasker_take_home_pay', 'locale', 'trimmed_address'
            ]
            
            # Extract only the required columns
            tasks_df = df[tasks_columns].copy()
            
            # Ensure latest_schedule_start_at is timezone-naive (as received from CSV)
            if 'latest_schedule_start_at' in tasks_df.columns:
                # Ensure it's timezone-naive (should be from convert_timezone)
                if tasks_df['latest_schedule_start_at'].dt.tz is not None:
                    # Remove timezone if present
                    tasks_df['latest_schedule_start_at'] = tasks_df['latest_schedule_start_at'].dt.tz_localize(None)
                
                # Log what we're about to insert
                sample_before = tasks_df['latest_schedule_start_at'].iloc[0] if len(tasks_df) > 0 else None
                if sample_before is not None and pd.notna(sample_before):
                    logger.info(f"Sample timestamp before SQL insert (timezone-naive): {sample_before}")

            if 'locale' in tasks_df.columns:
                # Normalize locale values - extract first 2 characters if longer, default to 'en'
                def normalize_locale(locale_val):
                    if pd.isna(locale_val) or locale_val == '':
                        return 'en'
                    locale_str = str(locale_val).lower()
                    if 'en' in locale_str:
                        return 'en'
                    elif 'es' in locale_str:
                        return 'es'
                    elif 'fr' in locale_str:
                        return 'fr'
                    elif 'de' in locale_str:
                        return 'de'
                    elif 'it' in locale_str:
                        return 'it'
                    else:
                        return 'en'
                
                tasks_df['locale'] = tasks_df['locale'].apply(normalize_locale)

            if 'trimmed_address' in tasks_df.columns:
                # Fill NaN values with empty string
                tasks_df['trimmed_address'] = tasks_df['trimmed_address'].fillna('')
                
                # Split by comma and take first part, then remove apt/unit/etc
                def process_address(address):
                    if pd.isna(address) or address == '':
                        return ''
                    # Split by comma and take first part
                    address_str = str(address).split(',')[0].strip()
                    # Remove apt/unit/etc if present
                    if any(word in address_str.lower() for word in ['apt', 'unit', 'suite', 'building', 'floor', 'room', 'apartment', 'apt.']):
                        return self.remove_apt_from_address(address_str)
                    return address_str
                
                tasks_df['trimmed_address'] = tasks_df['trimmed_address'].apply(process_address)
            
            # No need to remove duplicates - each job_id should be unique
            # The database will handle any primary key constraints
            
            # Insert data into tasks table
            if_exists_mode = 'replace' if replace_existing else 'append'
            
            # Insert using standard method - timestamps will be stored as-is (timezone-naive)
            tasks_df.to_sql(
                self.tasks_table, 
                self.engine, 
                if_exists=if_exists_mode, 
                index=False,
                method='multi'
            )
            
            logger.info(f"Successfully populated {self.tasks_table} with {len(tasks_df)} records")
            
            # Verify what was actually stored by querying back
            if 'latest_schedule_start_at' in tasks_df.columns and len(tasks_df) > 0:
                try:
                    with self.engine.connect() as conn:
                        result = conn.execute(text(f"SELECT latest_schedule_start_at FROM {self.tasks_table} LIMIT 1"))
                        stored_value = result.fetchone()[0]
                        logger.info(f"Verified stored timestamp: {stored_value}")
                        conn.commit()
                except Exception as e:
                    logger.warning(f"Could not verify stored timestamp: {e}")
            
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
            
            # Add locale if it exists in the dataframe
            if 'locale' in df.columns:
                tasker_columns.append('locale')
            
            # Extract only the required columns
            tasker_df = df[tasker_columns].copy()
            
            # Remove duplicates based on tasker_id (each tasker should appear only once)
            tasker_df = tasker_df.drop_duplicates(subset=['tasker_id'])
            
            # Normalize locale values if locale column exists
            if 'locale' in tasker_df.columns:
                def normalize_locale(locale_val):
                    if pd.isna(locale_val) or locale_val == '':
                        return 'en'
                    locale_str = str(locale_val).lower()
                    if 'en' in locale_str:
                        return 'en'
                    elif 'es' in locale_str:
                        return 'es'
                    elif 'fr' in locale_str:
                        return 'fr'
                    elif 'de' in locale_str:
                        return 'de'
                    elif 'it' in locale_str:
                        return 'it'
                    else:
                        return 'en'
                
                tasker_df['locale'] = tasker_df['locale'].apply(normalize_locale)
            
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
            'description', 'duration_hours', 'tasker_take_home_pay', 'locale', 'trimmed_address'
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

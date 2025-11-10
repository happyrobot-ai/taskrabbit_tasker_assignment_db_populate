# TaskRabbit Tasker Assignment Database Populator

This project reads data from a CSV file and populates two PostgreSQL tables in a DBeaver database.

## CSV Data Mapping

The script maps CSV columns to two database tables:

### Tasks Table (`taskrabbit_tasks_1` or `test_task_rabbit_tasks_1`)
- tasker_id
- metro_name
- job_id
- postal_code
- latitude
- longitude
- country_key
- latest_schedule_start_at
- time_zone
- is_job_bundle
- is_assigned
- is_accepted
- is_scheduled
- marketplace_key
- description
- duration_hours
- tasker_take_home_pay

### Tasker Data Table (`taskrabbit_tasker_data_1` or `test_taskrabbit_tasker_data_1`)
- tasker_id
- name
- email
- phone_number
- tenure_months
- lifetime_submitted_invoices_bucket

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file based on `env_example.txt` and fill in your database credentials:
```bash
cp env_example.txt .env
```

3. Update the `.env` file with your actual database connection details.

## Usage

### Production Tables (Default)
```bash
python db_populator.py --csv-path /path/to/your/csv/file.csv
```

### Test Tables
```bash
python db_populator.py --csv-path /path/to/your/csv/file.csv --test
```

### Custom Environment File
```bash
python db_populator.py --csv-path /path/to/your/csv/file.csv --env-file /path/to/custom/.env
```

## Command Line Arguments

- `--csv-path`: **Required** - Path to the CSV file to process
- `--test`: **Optional** - Use test tables instead of production tables
- `--env-file`: **Optional** - Path to custom .env file (default: .env)

## Configuration

The script uses environment variables for database configuration:
- `DB_HOST`: Database host address
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password

## Features

- **Automatic duplicate removal**: Removes duplicates based on `tasker_id` for tasker data table only
- **Full job data**: Keeps all job records in tasks table (job_id should be unique)
- **Column validation**: Validates that all required CSV columns are present
- **Test/Production mode**: Automatically selects appropriate table names
- **Comprehensive logging**: Detailed logging for troubleshooting
- **Error handling**: Graceful error handling with informative messages

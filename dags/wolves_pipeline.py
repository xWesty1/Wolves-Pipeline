#!/usr/bin/env python3
"""
Minnesota Timberwolves Data Pipeline DAG

This DAG extracts NBA data for the Minnesota Timberwolves and loads it into Snowflake.
Runs daily at 6 AM ET (11 UTC) starting June 1, 2025.
"""

import os
import subprocess
import snowflake.connector
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='wolves_pipeline',
    default_args=default_args,
    description='Minnesota Timberwolves NBA data extraction and loading to Snowflake',
    schedule='0 11 * * *',  # 6 AM ET = 11 UTC
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['nba', 'timberwolves', 'snowflake'],
)
def wolves_pipeline():
    """
    DAG for extracting Timberwolves data and loading to Snowflake
    """
    
    @task(task_id='extract_and_load_data')
    def extract_and_load_data():
        """
        Extract NBA data using wolves_extractor.py and load to Snowflake
        """
        
        # Set environment variables
        env = os.environ.copy()
        env['TEAM_ID'] = '1610612750'
        env['SEASON'] = '2024-25'
        
        # Get today's date for file path
        today_str = datetime.now().strftime('%Y%m%d')
        data_dir = Path(f"data/raw/{today_str}")
        
        try:
            # Step 1: Run wolves_extractor.py via subprocess
            print("Running wolves_extractor.py...")
            
            # Get the path to the extractor script (in Astronomer include directory)
            script_dir = Path("/usr/local/airflow/include/nba-twolves-pipeline")
            extractor_path = script_dir / "wolves_extractor.py"
            
            # Change to the nba-twolves-pipeline directory to run the script
            original_cwd = os.getcwd()
            os.chdir(script_dir)
            
            # Run the extractor
            print(f"Current working directory before running extractor: {os.getcwd()}")
            result = subprocess.run(
                ['python', 'wolves_extractor.py'],
                env=env,
                capture_output=True,
                text=True,
                check=True
            )
            
            print(f"Extractor output: {result.stdout}")
            if result.stderr:
                print(f"Extractor stderr: {result.stderr}")
            
            # Check if files were created
            expected_data_dir = Path(f"data/raw/{today_str}")
            print(f"Checking for files in: {expected_data_dir}")
            print(f"Directory exists: {expected_data_dir.exists()}")
            if expected_data_dir.exists():
                files_in_dir = list(expected_data_dir.iterdir())
                print(f"Files in data directory: {files_in_dir}")
                
                # Check if required files exist
                required_files = ['games.csv', 'player_stats.csv']
                missing_files = []
                for file_name in required_files:
                    file_path = expected_data_dir / file_name
                    if not file_path.exists():
                        missing_files.append(file_name)
                
                if missing_files:
                    raise AirflowException(f"Extractor failed to create required files: {missing_files}")
            else:
                raise AirflowException("Extractor failed - no data directory created. Check extractor logs above for API errors.")
            
            # Change back to original directory
            os.chdir(original_cwd)
            
            # Step 2: Connect to Snowflake
            print("Connecting to Snowflake...")
            
            try:
                # Get connection details from Airflow connection
                snowflake_conn = BaseHook.get_connection('snowflake_default')
                
                # For Generic connection type, account might be in host or extra field
                account = (
                    snowflake_conn.extra_dejson.get('account') or 
                    snowflake_conn.host or 
                    'bcuhkxz-yc43329'  # full account identifier from URL
                )
                
                print(f"Using Snowflake account: {account}")
                print(f"Connection type: {snowflake_conn.conn_type}")
                
                # Establish Snowflake connection
                conn = snowflake.connector.connect(
                    account=account,
                    user=snowflake_conn.login,
                    password=snowflake_conn.password,
                    role='ACCOUNTADMIN',
                    warehouse='ETL_WH',
                    database='NBA',
                    schema='RAW'
                )
            except Exception as e:
                raise AirflowException(f"Failed to get Snowflake connection 'snowflake_default': {e}")
            
            cursor = conn.cursor()
            
            # Step 3: Process each CSV file (games and player_stats)
            file_names = ['games', 'player_stats']
            table_mapping = {
                'games': 'GAMES',
                'player_stats': 'PLAYER_STATS'
            }
            
            for file_name in file_names:
                print(f"Processing {file_name}.csv...")
                
                # Construct full file path
                csv_file_path = script_dir / "data" / "raw" / today_str / f"{file_name}.csv"
                
                # Verify file exists
                if not csv_file_path.exists():
                    raise AirflowException(f"Expected CSV file not found: {csv_file_path}")
                
                # Step 4: PUT file to Snowflake stage
                put_command = f"""
                PUT file://{csv_file_path.as_posix()} @CSV_STAGE 
                AUTO_COMPRESS=TRUE
                """
                
                print(f"Executing PUT command for {file_name}...")
                cursor.execute(put_command)
                put_result = cursor.fetchall()
                print(f"PUT result for {file_name}: {put_result}")
                
                # Step 5: COPY INTO table from staged file
                table_name = table_mapping[file_name]
                copy_command = f"""
                COPY INTO {table_name}
                FROM @CSV_STAGE/{csv_file_path.name}.gz
                FILE_FORMAT = CSV_FMT
                FORCE = TRUE
                ON_ERROR = 'ABORT_STATEMENT'
                """
                
                print(f"Executing COPY INTO command for {table_name}...")
                cursor.execute(copy_command)
                copy_result = cursor.fetchall()
                print(f"COPY INTO result for {table_name}: {copy_result}")
            
            # Close Snowflake connection
            cursor.close()
            conn.close()
            
            print("Successfully completed data extraction and loading to Snowflake!")
            
        except subprocess.CalledProcessError as e:
            raise AirflowException(f"wolves_extractor.py failed: {e.stdout}\n{e.stderr}")
        except snowflake.connector.Error as e:
            raise AirflowException(f"Snowflake error: {e}")
        except Exception as e:
            raise AirflowException(f"Unexpected error: {e}")
    
    # Define task dependencies
    extract_and_load_data()


# Instantiate the DAG
dag_instance = wolves_pipeline() 
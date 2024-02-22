#!/bin/bash

# Define the base URL for 2019
base_url_2019="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-"

# Define the command
command="python ingest_fhv_data.py --user=root --password=root --host=localhost --port=5432 --db=nyc_taxi_data --table_name=fhv_trips --csv_file_url="

# Function to print and run the command
run_command() {
    local url=$1
    echo "Running command: $command$url"
    $command$url
    echo "Command completed"
}

# Loop through the months from 01 to 12 for the year 2019
for ((month=1; month<=12; month++))
do
    # Format the URL with the year and month
    url="${base_url_2019}$(printf "%02d" $month).csv.gz"
    
    # Run the command
    run_command $url
    
    # Add a sleep to prevent overwhelming the server, if needed
    sleep 1
done

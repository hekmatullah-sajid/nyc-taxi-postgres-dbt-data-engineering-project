#!/bin/bash

# Define the base URL for CSV Files
base_url_csv="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_"

# Define the command
command="python ingest_yellow_taxi_data.py --user=root --password=root --host=localhost --port=5432 --db=nyc_taxi_data --table_name=yellow_taxi_trips --csv_file_url="

# Function to print and run the command
run_command() {
    local url=$1
    echo "Running command: $command$url"
    $command$url
    echo "Command completed"
}

# Loop through the years 2019 and 2020
for year in 2019 2020
do
    base_url=${base_url_csv}
    if [ $year -eq 2020 ]; then
        base_url=${base_url_csv}
    fi

    # Loop through the months from 01 to 12 for the current year
    for ((month=1; month<=12; month++))
    do
        # Format the URL with the year and month
        url="${base_url}${year}-$(printf "%02d" $month).csv.gz"
        
        # Run the command
        run_command $url
        
        # Add a sleep to prevent overwhelming the server, if needed
        sleep 1
    done
done

#!/bin/bash

# Define the base URL for 2019
base_url_2019="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_"

# Define the base URL for 2020
base_url_2020="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_"

# Define the command
command="python ingest_green_taxi_data.py --user=root --password=root --host=localhost --port=5432 --db=nyc_taxi_data --table_name=green_taxi_trips --csv_file_url="

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
    base_url=${base_url_2019}
    if [ $year -eq 2020 ]; then
        base_url=${base_url_2020}
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

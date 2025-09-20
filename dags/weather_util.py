import json
import boto3
import requests
import datetime
import csv
import io

def fetch_weather_data_for_city(api_url_template, city, api_key):
    url = api_url_template.format(city=city, api_key=api_key)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def extract_csv_data(city, weather_data):
    return {
        "city": city,
        "timestamp": weather_data.get("dt"),
        "weather_main": weather_data["weather"][0]["main"] if "weather" in weather_data and weather_data["weather"] else None,
        "weather_description": weather_data["weather"][0]["description"] if "weather" in weather_data and weather_data["weather"] else None,
        "temp": weather_data["main"]["temp"],
        "feels_like": weather_data["main"]["feels_like"],
        "humidity": weather_data["main"]["humidity"],
        "wind_speed": weather_data["wind"]["speed"],
        "clouds": weather_data["clouds"]["all"]
    }

def convert_list_to_csv_string(data_list):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data_list[0].keys())
    writer.writeheader()
    writer.writerows(data_list)
    return output.getvalue()

def upload_csv_to_s3(s3_client, bucket, prefix, csv_string):
    now = datetime.datetime.utcnow()
    file_name = f"weather_{now.strftime('%Y%m%d%H%M%S')}.csv"
    s3_path = f"{prefix}/{file_name}"
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_path,
        Body=csv_string,
        ContentType="text/csv"
    )
    return s3_path

def fetch_weather_data_save_csv():
    api_key = "3916125fbf25bdb1b958c1f65c7ad4e9"  # Replace with your actual API key
    cities = ["New York"]
    api_url_template = "https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

    s3_client = boto3.client("s3")
    print("S3 client initialized.")
    all_city_data = []


    
    for city in cities:
        try:
            weather_data = fetch_weather_data_for_city(api_url_template, city, api_key)
            #print(f"Weather data fetched for {city}: {weather_data}")
            city_data = extract_csv_data(city, weather_data)
            all_city_data.append(city_data)
            #print(f"Data fetched for {city}: {city_data}")
        except Exception as e:
            print(f"Error fetching data for {city}: {e}")

    if not all_city_data:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "No data fetched for any city."})
        }

    csv_string = convert_list_to_csv_string(all_city_data)
    print(csv_string)

    s3_bucket = "snowflake-june2025"
    s3_folder_name = "weather_data/unprocessed/"
    s3_path = upload_csv_to_s3(s3_client, s3_bucket, s3_folder_name, csv_string)
    print(f"CSV uploaded to S3 at {s3_path}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Weather data for multiple cities fetched and uploaded as CSV to S3.",
            "s3_path": s3_path
        })
    }


if __name__ == "__main__":
    fetch_weather_data_save_csv()

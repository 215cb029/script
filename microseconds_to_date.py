from datetime import datetime, timedelta, timezone

def convert_microseconds_to_date_and_relative_time(microseconds):
    timestamp_seconds = microseconds / 1_000_000
    timestamp_dt = datetime.fromtimestamp(timestamp_seconds, timezone.utc)
    time_zone_offset = timedelta(hours=5, minutes=30)
    date_time = timestamp_dt + time_zone_offset
    date_time=date_time.replace(microsecond=0, tzinfo=None)
    print(date_time)
    return date_time
   # return f"Your time zone: {formatted_date}\nRelative: {days_ago}"

# Example usage
timestamp_microseconds = 1751790423222
result = convert_microseconds_to_date_and_relative_time(timestamp_microseconds)
print(result)

from datetime import datetime, timedelta, timezone
import math

def get_day_difference_from_today(date_time):
    # Get current time in UTC
    now = datetime.now(timezone.utc)
    date_time = datetime.fromisoformat(date_time)
    # Convert UTC to your local timezone (UTC+5:30)
    time_zone_offset = timedelta(hours=5, minutes=30)
    now_localized = now + time_zone_offset

    print(f"Current localized time: {now_localized}")

    # Adjust input datetime to same local timezone
    date_time_localized = date_time + time_zone_offset

    # Calculate total time difference
    time_diff = now_localized - date_time_localized

    # Convert total seconds to days and round up
    day_difference = math.ceil(time_diff.total_seconds() / 86400)

    print(f"{day_difference} day{'s' if day_difference != 1 else ''} ago")

    return day_difference
if __name__ == "__main__":
    get_day_difference_from_today('2025-04-01 12:53:18.709348+00:00')
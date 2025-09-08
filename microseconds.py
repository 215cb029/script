from datetime import datetime
import pytz

# Input datetime string
dt_str = "2025-04-05 08:30:25"

# Parse the datetime string to a datetime object in UTC
dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
dt = pytz.utc.localize(dt)  # Make sure it's in UTC

# Convert to Unix timestamp (in microseconds)
microseconds = int(dt.timestamp() * 1_000_000)

print(microseconds)

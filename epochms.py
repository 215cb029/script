from datetime import datetime, timedelta, timezone

# IST is UTC +5:30
IST_OFFSET = timedelta(hours=5, minutes=30)
IST = timezone(IST_OFFSET)

# Input strings in IST
ist_from_str = "2025-07-07 11:52:36"
ist_to_str = "2025-07-07 14:52:36"

# Parse to datetime with IST timezone
from_ist = datetime.strptime(ist_from_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
to_ist = datetime.strptime(ist_to_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)

# Convert to UTC
from_utc = from_ist.astimezone(timezone.utc)
to_utc = to_ist.astimezone(timezone.utc)

# Format to ISO 8601 with milliseconds and 'Z'
from_str = from_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
to_str = to_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

# Output
print(f'from: "{from_str}", to: "{to_str}"')

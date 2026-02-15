"""
Report filter constants – used by weekend bot, split_weekend_trips, and daily reports.
Adjust these to change what qualifies as "bookable" for your needs.
"""
# Minimum seats required (2 = couples/friends traveling together)
MIN_SEATS = 2

# Weekend trips: outbound–inbound gap in days (3–4 = typical long weekend)
TRIP_DAYS_MIN = 3
TRIP_DAYS_MAX = 4

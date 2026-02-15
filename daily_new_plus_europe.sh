#!/usr/bin/env bash
# ~/sas_awards/daily_new_plus_europe.sh

cd ~/sas_awards

LOG=run.log
OUT=~/OneDrive/SASReports/new_plus_europe_$(date +%Y-%m-%d).csv

# Header row
echo "direction,date,airport_code,city_name,country_name,plus_seats" > "$OUT"

# Extract the "🆕 Added flights:" block, then for each line with AP>0:
awk '/🆕 Added flights:/{inBlock=1; next} /^$/{inBlock=0} inBlock' "$LOG" | \
while read -r bullet date direction code rest; do
  # grab AP value
  ap=$(echo "$rest" | grep -o 'AP=[0-9]\+' | cut -d= -f2)
  if [ -z "$ap" ] || [ "$ap" -le 0 ]; then
    continue
  fi

  # lookup city & country in the DB
  read city country <<< "$(sqlite3 sas_awards.sqlite -separator '||' -noheader \
    "SELECT city_name, country_name
       FROM flights
      WHERE airport_code='$code'
        AND direction='$direction'
        AND date='$date'
      LIMIT 1;")"

  # only include European countries
  case "$country" in
    Austria|Belgium|Denmark|France|Germany|Ireland|Italy|Netherlands|Norway|Portugal|Spain|Sweden|Switzerland|United\ Kingdom)
      echo "$direction,$date,$code,$city,$country,$ap" >> "$OUT"
      ;;
  esac
done

echo "Written $OUT"

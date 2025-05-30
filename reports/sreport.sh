#!/bin/bash
# dumitru-florin.idita@eviden.com

# Script to calculate Slurm Cluster CPU Time Availability
# It fetches data from 'sreport cluster utilization' for a specified day, month, or year.
# Version 5: Handles YYYY, YYYY-MM, YYYY-MM-DD input formats and outputs percentages with comma decimal.

set -o pipefail # Exit if any command in a pipeline fails

# --- Function to display usage ---
usage() {
    echo "Usage: $0 <YYYY|YYYY-MM|YYYY-MM-DD>"
    echo "Calculates Slurm cluster CPU time availability for the specified period."
    echo "Examples:"
    echo "  $0 2025          (for the entire year 2025)"
    echo "  $0 2025-04       (for April 2025)"
    echo "  $0 2025-04-15    (for the day 2025-04-15)"
    exit 1
}

# --- Argument Validation ---
if [ -z "$1" ]; then
    echo "Error: Date argument is required."
    usage
fi

DATE_ARG=$1
FETCH_PERIOD_DISPLAY=""
START_DATE=""
END_DATE=""

# Determine date format and set START_DATE and END_DATE
if [[ "$DATE_ARG" =~ ^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$ ]]; then
    # YYYY-MM-DD format
    YEAR=$(echo "$DATE_ARG" | cut -d'-' -f1)
    MONTH=$(echo "$DATE_ARG" | cut -d'-' -f2)
    DAY=$(echo "$DATE_ARG" | cut -d'-' -f3)

    if ! date -d "$DATE_ARG" >/dev/null 2>&1; then
        echo "Error: Invalid date specified: $DATE_ARG"
        usage
    fi

    START_DATE=$DATE_ARG
    END_DATE=$(date -d "$START_DATE + 1 day" +%Y-%m-%d)
    if [ $? -ne 0 ]; then
        echo "Error: Could not calculate end date. Ensure GNU 'date' command is available and supports '-d' option."
        exit 1
    fi
    FETCH_PERIOD_DISPLAY="for the day $DATE_ARG"
elif [[ "$DATE_ARG" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]]; then
    # YYYY-MM format
    YEAR=$(echo "$DATE_ARG" | cut -d'-' -f1)
    MONTH=$(echo "$DATE_ARG" | cut -d'-' -f2)
    START_DATE="${YEAR}-${MONTH}-01"

    if [ "$MONTH" == "12" ]; then
        NEXT_MONTH_YEAR=$((YEAR + 1))
        NEXT_MONTH="01"
    else
        NEXT_MONTH_YEAR=$YEAR
        NEXT_MONTH_NUM=$((10#$MONTH + 1))
        NEXT_MONTH=$(printf "%02d" "$NEXT_MONTH_NUM")
    fi
    END_DATE="${NEXT_MONTH_YEAR}-${NEXT_MONTH}-01"
    FETCH_PERIOD_DISPLAY="for the month $DATE_ARG"
elif [[ "$DATE_ARG" =~ ^[0-9]{4}$ ]]; then
    # YYYY format
    YEAR=$DATE_ARG
    START_DATE="${YEAR}-01-01"
    NEXT_YEAR=$((YEAR + 1))
    END_DATE="${NEXT_YEAR}-01-01"
    FETCH_PERIOD_DISPLAY="for the year $DATE_ARG"
else
    echo "Error: Invalid date format. Please use YYYY, YYYY-MM, or YYYY-MM-DD."
    usage
fi

echo "-----------------------------------------------------------"
echo "Slurm Cluster CPU Time Availability Calculator"
echo "-----------------------------------------------------------"
echo "Fetching Slurm utilization report $FETCH_PERIOD_DISPLAY (from $START_DATE to $END_DATE)..."
echo ""

# --- Execute sreport and Capture Output ---
SREPORT_USED_PARSABLE2=0

SREPORT_OUTPUT_PRIMARY=$(sreport cluster utilization start="$START_DATE" end="$END_DATE" -t Minutes --parsable2 2>&1)
SREPORT_EXIT_CODE_PRIMARY=$?

if [ $SREPORT_EXIT_CODE_PRIMARY -eq 0 ] && \
   ! [[ "$SREPORT_OUTPUT_PRIMARY" == *"Invalid option"* ]] && \
   ! [[ "$SREPORT_OUTPUT_PRIMARY" == *"sreport: error:"* ]] && \
   [[ "$SREPORT_OUTPUT_PRIMARY" == *"|"* ]]; then
    SREPORT_USED_PARSABLE2=1
    SREPORT_OUTPUT="$SREPORT_OUTPUT_PRIMARY"
    SREPORT_FINAL_EXIT_CODE=$SREPORT_EXIT_CODE_PRIMARY
  #  echo "Info: Using sreport --parsable2 output."
else
    echo "Warning: 'sreport ... -t Minutes --parsable2' failed, returned an error, did not produce pipe-delimited output, or is not supported."
    if [ $SREPORT_EXIT_CODE_PRIMARY -ne 0 ]; then
        echo "         Primary attempt exit code: $SREPORT_EXIT_CODE_PRIMARY"
    fi
    echo "         Primary attempt output snippet (first 3 lines):"
    echo "$SREPORT_OUTPUT_PRIMARY" | head -n 3
    echo "         Retrying with basic sreport command (space-delimited output expected)."

    SREPORT_OUTPUT_FALLBACK=$(sreport cluster utilization start="$START_DATE" end="$END_DATE" 2>&1)
    SREPORT_FINAL_EXIT_CODE=$?
    SREPORT_OUTPUT="$SREPORT_OUTPUT_FALLBACK"
    SREPORT_USED_PARSABLE2=0
    echo "Info: Using basic sreport output."
fi

if [ $SREPORT_FINAL_EXIT_CODE -ne 0 ]; then
    echo "Error: sreport command failed with exit code $SREPORT_FINAL_EXIT_CODE."
    echo "sreport output (from last attempt):"
    echo "$SREPORT_OUTPUT"
    exit 1
fi

AWK_SCRIPT='
    function parse_suffixed_value(str) {
        val = str
        suffix = ""
        multiplier = 1
        if (match(tolower(str), /[kmgtp]$/)) {
            suffix = substr(str, RSTART, RLENGTH)
            val = substr(str, 1, RSTART-1)
        }
        if (suffix == "k") multiplier = 1000
        else if (suffix == "m") multiplier = 1000 * 1000
        else if (suffix == "g") multiplier = 1000 * 1000 * 1000
        else if (suffix == "t") multiplier = 1000 * 1000 * 1000 * 1000
        gsub(/,/, "", val)
        return val * multiplier
    }
    /^-+$/ {next}
    /^Usage reported in CPU Minutes/ {next}
    /^Cluster Utilization .* - .*/ {next}
    /^Cluster\|Allocated\|Down\|PLND Down\|Idle\|(Planned|Reserved)\|Reported/ {next}
    /^Cluster\s+(Allocated|Alloc)\s+(Down)\s+(PLND Down|PlndDown)\s+(Idle)\s+(Planned|TresUsed)\s+(Reported|Rept)/ {next}
    /^\s*$/ {next}
    NF >= 7 && $1 !~ /^[0-9]+(\.[0-9]+)?$/ && \
    $2 ~ /^[0-9.,]+([KMGTPkmgtp])?$/ && $3 ~ /^[0-9.,]+([KMGTPkmgtp])?$/ && $4 ~ /^[0-9.,]+([KMGTPkmgtp])?$/ && \
    $5 ~ /^[0-9.,]+([KMGTPkmgtp])?$/ && $6 ~ /^[0-9.,]+([KMGTPkmgtp])?$/ && $7 ~ /^[0-9.,]+([KMGTPkmgtp])?$/ {
        reported_raw = $7
        down_raw = $3
        plnd_down_raw = $4
        print parse_suffixed_value(reported_raw), parse_suffixed_value(down_raw), parse_suffixed_value(plnd_down_raw)
        exit
    }
'

if [ "$SREPORT_USED_PARSABLE2" -eq 1 ]; then
    PARSED_VALUES=$(echo "$SREPORT_OUTPUT" | awk -F'|' "$AWK_SCRIPT")
else
    PARSED_VALUES=$(echo "$SREPORT_OUTPUT" | awk "$AWK_SCRIPT")
fi

if [ -z "$PARSED_VALUES" ]; then
    echo "Error: Could not parse the required data from sreport output."
    echo "This might be due to an unexpected sreport output format or no data for the period."
    echo "Please check the sreport output manually:"
    echo "-------------------- SREPORT OUTPUT START --------------------"
    echo "$SREPORT_OUTPUT"
    echo "--------------------- SREPORT OUTPUT END ---------------------"
    exit 1
fi

read -r REPORTED_CPU_MINUTES DOWN_CPU_MINUTES PLND_DOWN_CPU_MINUTES <<< "$PARSED_VALUES"

if [ -z "$REPORTED_CPU_MINUTES" ] || [ -z "$DOWN_CPU_MINUTES" ] || [ -z "$PLND_DOWN_CPU_MINUTES" ]; then
    echo "Error: Failed to assign all parsed values. One or more values are empty."
    echo "Parsed string was: '$PARSED_VALUES'"
    echo "This indicates a parsing logic failure. Please review sreport output:"
    echo "-------------------- SREPORT OUTPUT START --------------------"
    echo "$SREPORT_OUTPUT"
    echo "--------------------- SREPORT OUTPUT END ---------------------"
    exit 1
fi

for val_check in "$REPORTED_CPU_MINUTES" "$DOWN_CPU_MINUTES" "$PLND_DOWN_CPU_MINUTES"; do
    if ! [[ "$val_check" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        echo "Error: One of the parsed values ('$val_check') is not a valid number after suffix conversion."
        echo "Reported: '$REPORTED_CPU_MINUTES', Down: '$DOWN_CPU_MINUTES', PLND Down: '$PLND_DOWN_CPU_MINUTES'"
        exit 1
    fi
done

echo "Successfully parsed and converted values from sreport:"
echo "  Reported CPU Minutes:     $REPORTED_CPU_MINUTES"
echo "  Down CPU Minutes:         $DOWN_CPU_MINUTES"
echo "  PLND Down CPU Minutes:    $PLND_DOWN_CPU_MINUTES"
echo ""
echo "Calculating..."
echo "-----------------------------------------------------------"

UNAVAILABLE_CPU_TIME_TOTAL=$(echo "$DOWN_CPU_MINUTES + $PLND_DOWN_CPU_MINUTES" | bc)
AVAILABLE_CPU_TIME_TOTAL=$(echo "$REPORTED_CPU_MINUTES - $UNAVAILABLE_CPU_TIME_TOTAL" | bc)
PERCENTAGE_AVAILABLE_TOTAL_FORMATTED_DOT="N/A" # Intermediate variable with dot

if [ "$(bc <<< "$REPORTED_CPU_MINUTES == 0")" -eq 1 ]; then
    echo "Warning: 'Reported' CPU minutes is zero. Cannot calculate percentages."
elif [ "$(bc <<< "$AVAILABLE_CPU_TIME_TOTAL < 0")" -eq 1 ]; then
    echo "Warning: Calculated 'Total Available CPU Time' ($AVAILABLE_CPU_TIME_TOTAL) is negative."
    PERCENTAGE_AVAILABLE_TOTAL_FORMATTED_DOT="Error (Negative Available Time)"
else
    PERCENTAGE_AVAILABLE_TOTAL=$(bc -l <<< "scale=4; if($REPORTED_CPU_MINUTES > 0) { ($AVAILABLE_CPU_TIME_TOTAL / $REPORTED_CPU_MINUTES) * 100 } else { 0 }")
    PERCENTAGE_AVAILABLE_TOTAL_FORMATTED_DOT=$(printf "%.2f" "$PERCENTAGE_AVAILABLE_TOTAL")
fi
# Replace dot with comma for final display
PERCENTAGE_AVAILABLE_TOTAL_FORMATTED=${PERCENTAGE_AVAILABLE_TOTAL_FORMATTED_DOT//./,}


AVAILABLE_CPU_TIME_EXCL_PLANNED=$(echo "$REPORTED_CPU_MINUTES - $DOWN_CPU_MINUTES" | bc)
PERCENTAGE_AVAILABLE_EXCL_PLANNED_FORMATTED_DOT="N/A" # Intermediate variable with dot

if [ "$(bc <<< "$REPORTED_CPU_MINUTES == 0")" -eq 1 ]; then
    :
elif [ "$(bc <<< "$AVAILABLE_CPU_TIME_EXCL_PLANNED < 0")" -eq 1 ]; then
    echo "Warning: Calculated 'Available CPU Time (Ignoring Planned Downtime)' ($AVAILABLE_CPU_TIME_EXCL_PLANNED) is negative."
    PERCENTAGE_AVAILABLE_EXCL_PLANNED_FORMATTED_DOT="Error (Negative Available Time)"
else
    PERCENTAGE_AVAILABLE_EXCL_PLANNED=$(bc -l <<< "scale=4; if($REPORTED_CPU_MINUTES > 0) { ($AVAILABLE_CPU_TIME_EXCL_PLANNED / $REPORTED_CPU_MINUTES) * 100 } else { 0 }")
    PERCENTAGE_AVAILABLE_EXCL_PLANNED_FORMATTED_DOT=$(printf "%.2f" "$PERCENTAGE_AVAILABLE_EXCL_PLANNED")
fi
# Replace dot with comma for final display
PERCENTAGE_AVAILABLE_EXCL_PLANNED_FORMATTED=${PERCENTAGE_AVAILABLE_EXCL_PLANNED_FORMATTED_DOT//./,}


echo "Reported CPU Minutes:               $REPORTED_CPU_MINUTES"
echo "Unplanned Down CPU Minutes:         $DOWN_CPU_MINUTES"
echo "Planned Down CPU Minutes:           $PLND_DOWN_CPU_MINUTES"
echo "-----------------------------------------------------------"
echo "Total Unavailable CPU Time:         $UNAVAILABLE_CPU_TIME_TOTAL"
echo "(Unplanned + Planned Down)"
echo ""
echo "Total Available CPU Time:           $AVAILABLE_CPU_TIME_TOTAL"
echo "(Reported - Total Unavailable)"
echo ""
echo "Available CPU Time (Ignoring Planned): $AVAILABLE_CPU_TIME_EXCL_PLANNED"
echo "(Reported - Unplanned Down Only)"
echo "-----------------------------------------------------------"

if [[ "$PERCENTAGE_AVAILABLE_TOTAL_FORMATTED" == "N/A" || "$PERCENTAGE_AVAILABLE_TOTAL_FORMATTED" == *"Error"* ]]; then # Check for "Error" substring
    echo "Percentage of CPU Time Available:   $PERCENTAGE_AVAILABLE_TOTAL_FORMATTED"
else
    echo "Percentage of CPU Time Available:   $PERCENTAGE_AVAILABLE_TOTAL_FORMATTED%"
fi

if [[ "$PERCENTAGE_AVAILABLE_EXCL_PLANNED_FORMATTED" == "N/A" || "$PERCENTAGE_AVAILABLE_EXCL_PLANNED_FORMATTED" == *"Error"* ]]; then # Check for "Error" substring
    echo "Percentage of CPU Time Available:   $PERCENTAGE_AVAILABLE_EXCL_PLANNED_FORMATTED  (Ignoring Planned Downtime)"
else
    echo "Percentage of CPU Time Available:   $PERCENTAGE_AVAILABLE_EXCL_PLANNED_FORMATTED% (Ignoring Planned Downtime)"
fi
echo "-----------------------------------------------------------"

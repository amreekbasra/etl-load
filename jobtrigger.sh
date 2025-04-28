#!/bin/bash

# Simple script to trigger an ETL job via the Workflow Engine API

# --- Configuration ---
# Adjust the URL to your running ETL Workflow Engine instance
ETL_ENGINE_API_URL="http://localhost:8080/api/etl/v1/jobs/submitFromFile"
# Default config file path (can be overridden by command line argument)
DEFAULT_CONFIG_FILE="./job_config.json"
# Timeout for the curl command (e.g., 10 seconds)
CURL_TIMEOUT=10

# --- Functions ---
log_info() {
  echo "[INFO] $(date +'%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
  echo "[ERROR] $(date +'%Y-%m-%d %H:%M:%S') - $1" >&2
}

# Function to URL encode a string (basic implementation)
urlencode() {
    local string="${1}"
    local strlen=${#string}
    local encoded=""
    local pos c o

    for (( pos=0 ; pos<strlen ; pos++ )); do
       c=${string:$pos:1}
       case "$c" in
          [-_.~a-zA-Z0-9] ) o="${c}" ;;
          * )               printf -v o '%%%02x' "'$c"
       esac
       encoded+="${o}"
    done
    echo "${encoded}"
}


# --- Main Script Logic ---

# Use command line argument if provided, otherwise use default
CONFIG_FILE="${1:-$DEFAULT_CONFIG_FILE}"

log_info "Starting ETL job trigger script..."
log_info "Using configuration file: $CONFIG_FILE"

# Check if config file exists and is readable
if [ ! -f "$CONFIG_FILE" ]; then
  log_error "Configuration file not found: $CONFIG_FILE"
  exit 1
fi
if [ ! -r "$CONFIG_FILE" ]; then
  log_error "Configuration file not readable: $CONFIG_FILE"
  exit 1
fi

# URL encode the config file path parameter
ENCODED_CONFIG_PATH=$(urlencode "$CONFIG_FILE")
TARGET_URL="${ETL_ENGINE_API_URL}?configPath=${ENCODED_CONFIG_PATH}"

log_info "Sending job submission request to: $ETL_ENGINE_API_URL"
log_info "Target URL with encoded path: $TARGET_URL"

# Make the API call using curl
# -s: Silent mode (hide progress meter)
# -X POST: Specify POST request method
# -w "%{http_code}": Output only the HTTP status code after the request
# --connect-timeout: Max time to connect
# -m: Max total time for the operation
HTTP_STATUS=$(curl -s -X POST -w "%{http_code}" --connect-timeout ${CURL_TIMEOUT} -m ${CURL_TIMEOUT} "${TARGET_URL}")
CURL_EXIT_CODE=$?

# Check curl exit code
if [ $CURL_EXIT_CODE -ne 0 ]; then
  log_error "curl command failed with exit code $CURL_EXIT_CODE. Could not connect to API or timeout occurred."
  exit 1
fi

log_info "Received HTTP status code: $HTTP_STATUS"

# Check HTTP status code
# 2xx codes are generally successful submissions (202 Accepted is ideal)
if [[ "$HTTP_STATUS" -ge 200 && "$HTTP_STATUS" -lt 300 ]]; then
  log_info "Job submission request successful (HTTP $HTTP_STATUS)."
  # Optional: Add logic here to poll the /status endpoint if needed
  # Example: poll_status $JOB_ID (would need to parse JOB_ID from response if API returned it)
  exit 0
else
  log_error "Job submission request failed with HTTP status code $HTTP_STATUS."
  # Optional: Attempt to read response body if API provides error details
  # curl -s -X POST "${TARGET_URL}" # (without -w to get body)
  exit 1
fi

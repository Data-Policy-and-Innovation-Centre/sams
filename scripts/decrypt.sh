#!/bin/bash

# ============= COMMAND-LINE ARGUMENTS ===========

# Paths
home="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$home/.."
project_root="$(pwd)"
cd "$project_root/data/raw"
echo "$(pwd)$"

# Handles errors by searching for a specific pattern in a log file.
#
# Parameters:
# - $1: The pattern to search for in the log file.
# - $2: The path to the log file.
#
# Returns:
# - If an error is found, prints the error message, the log file path, and exits with a status of 1.
# - If no error is found, does nothing.
handle_error () {
    local error=$(grep $1 "$2")
    if [ ! -z "$error" ]; then
        if [[ "$OSTYPE" == "darwin"* ]]; then
            say error
        fi
        echo "That code ran with error(s): ${error}"
        echo "Check logs at: ${2}"
        exit 1
    fi
}

# Decrypt
decrypt () {
	gpg --output placements.tar.gz --decrypt placements.tar.gz.gpg 2> gpg_error.log
	handle_error " failed " "gpg_error.log"
	rm gpg_error.log

	# Extract directory
	tar xvf placements.tar.gz
}

# Clean out unencrypted files
clean () {
	rm placements.tar.gz
	rm -r placements
}

while getopts ":cdq" opt; do
    case $opt in
        c)
            clean
            ;;
        d)
            decrypt
            ;;
        q)
            echo "Quitting..."
            exit 0
            ;;
        \?)
            echo "Invalid option. Use -c for clean, -d for decrypt, or -q to quit."
            show_menu
            ;;
    esac
    echo
done

# If no options were provided, prompt the user to select one
if [ $OPTIND -eq 1 ]; then
    echo "No options provided. Exiting..."
    exit 1
fi





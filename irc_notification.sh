#!/bin/bash
set -euo pipefail # Exit on error, treat unset variables as error, exit on pipe fails

# --- Configuration ---
# API Endpoint
API_URL="https://rust-info.gentoo.zip/api/v1/named_channels"

DATA_DIR="$(dirname "$0")/rust_version_data"
STABLE_FILE="$DATA_DIR/last_stable.txt"
BETA_FILE="$DATA_DIR/last_beta.txt"
NIGHTLY_FILE="$DATA_DIR/last_nightly.txt" # Will store "version (date)"

IRC_ENABLED=true # Set to false to disable IRC notifications for testing
IRC_SERVER="irc.libera.chat"
IRC_PORT="6667"
IRC_NICK="RustVersionBot$$"
IRC_USER="rustbot"
IRC_REALNAME="Rust Version Watcher"
IRC_CHANNEL="#gentoo-rust"

# --- Helper Functions ---
send_irc_message() {
    local message=$1
    if [[ "$IRC_ENABLED" != true ]]; then
        echo "IRC notification disabled."
        return 0
    fi

    echo "Sending IRC notification to $IRC_SERVER $IRC_CHANNEL..."
    # Basic IRC connection using netcat. Might need adjustments based on server/network.
    (
      echo "NICK $IRC_NICK"
      echo "USER $IRC_USER 0 * :$IRC_REALNAME"
      sleep 5 # Give server time to register nick/user
      echo "JOIN $IRC_CHANNEL"
      sleep 3 # Give server time to process join
      echo "PRIVMSG $IRC_CHANNEL :$message"
      sleep 2 # Allow message to be sent
      echo "QUIT"
    ) | nc -w 15 "$IRC_SERVER" "$IRC_PORT" # -w 15: timeout after 15 seconds

    if [ $? -ne 0 ]; then
      echo "Error: Failed to send IRC notification via nc." >&2
      return 1 # Indicate failure
    fi
    echo "IRC notification sent successfully."
    return 0
}

# --- Main Script ---
echo "Starting Rust version check: $(date)"

# Ensure data directory exists
mkdir -p "$DATA_DIR"
touch "$STABLE_FILE" "$BETA_FILE" "$NIGHTLY_FILE" # Ensure files exist even if empty

# Read old values (allow empty files on first run)
old_stable=$(cat "$STABLE_FILE")
old_beta=$(cat "$BETA_FILE")
old_nightly=$(cat "$NIGHTLY_FILE")

# Fetch new data
echo "Fetching data from $API_URL..."
json_data=$(curl --fail -sL "$API_URL") # -s: silent, -L: follow redirects, --fail: http errors are fatal

if [[ -z "$json_data" ]]; then
    echo "Error: Failed to fetch data or received empty response." >&2
    exit 1
fi

# Extract new values using jq
new_stable=$(echo "$json_data" | jq -r '.[] | select(.latest_stable) | .version // "Not Found"')
new_beta=$(echo "$json_data" | jq -r '.[] | select(.latest_beta) | .version // "Not Found"')
# Combine version and date for nightly for easier comparison
new_nightly=$(echo "$json_data" | jq -r '.[] | select(.latest_nightly) | "\(.version) (\(.release_date))" // "Not Found"')

# Check if extraction failed
if [[ "$new_stable" == "Not Found" || "$new_beta" == "Not Found" || "$new_nightly" == "Not Found" ]]; then
    echo "Error: Could not parse required version data from API response." >&2
    exit 1
fi

echo "Current Versions: Stable=$new_stable | Beta=$new_beta | Nightly=$new_nightly"

# --- Detect individual changes ---
stable_changed=false
beta_changed=false
nightly_changed=false
if [[ "$old_stable" != "$new_stable" ]]; then stable_changed=true; fi
if [[ "$old_beta" != "$new_beta" ]]; then beta_changed=true; fi
if [[ "$old_nightly" != "$new_nightly" ]]; then nightly_changed=true; fi

# --- Determine if notification is needed (Stable or Beta MUST have changed) ---
should_notify=false
if [[ "$stable_changed" == true || "$beta_changed" == true ]]; then
    should_notify=true
fi

notification_sent_successfully=true # Assume success if not attempted or disabled

# --- Send notification if needed ---
if [[ "$should_notify" == true ]]; then
    echo "Change detected in Stable or Beta. Preparing notification..."
    notification_parts=() # Use an array to build message parts

    if [[ "$stable_changed" == true ]]; then
        echo " - Stable changed: '$old_stable' -> '$new_stable'"
        notification_parts+=("Stable: $new_stable")
    fi
    if [[ "$beta_changed" == true ]]; then
        echo " - Beta changed: '$old_beta' -> '$new_beta'"
        notification_parts+=("Beta: $new_beta")
    fi
    # Include nightly info *only* if it also changed alongside stable/beta
    if [[ "$nightly_changed" == true ]]; then
         echo " - Nightly also changed: '$old_nightly' -> '$new_nightly'"
         notification_parts+=("Nightly: $new_nightly")
    fi

    # Construct the final message
    notification_message="Rust Update | $(printf '%s; ' "${notification_parts[@]}" | sed 's/; $//')"

    # Attempt to send notification
    if ! send_irc_message "$notification_message"; then
        notification_sent_successfully=false # Mark as failed
        echo "Warning: IRC notification failed." >&2
        # We will skip file updates below if this failed
    fi
else
    # No notification needed (Stable/Beta didn't change)
    echo "No changes detected in Stable or Beta."
    if [[ "$nightly_changed" == true ]]; then
        echo " - Nightly changed ('$old_nightly' -> '$new_nightly'), but no notification sent as Stable/Beta are unchanged."
    else
         echo " - Nightly also unchanged."
    fi
fi

# --- Update local files ---
# Update files if changes occurred AND (either no notification was needed OR notification was sent successfully)
# This ensures we don't update files if a required notification failed, allowing a retry next time.
# Importantly, this also ensures the nightly file IS updated if only nightly changed.
if [[ "$notification_sent_successfully" == true ]]; then
    updated_any_file=false
    if [[ "$stable_changed" == true ]]; then
        echo "Updating $STABLE_FILE"
        echo "$new_stable" > "$STABLE_FILE"
        updated_any_file=true
    fi
    if [[ "$beta_changed" == true ]]; then
        echo "Updating $BETA_FILE"
        echo "$new_beta" > "$BETA_FILE"
        updated_any_file=true
    fi
    if [[ "$nightly_changed" == true ]]; then
         # Update nightly file regardless of notification status, as long as notification didn't fail
         echo "Updating $NIGHTLY_FILE"
         echo "$new_nightly" > "$NIGHTLY_FILE"
         updated_any_file=true
    fi

    if [[ "$updated_any_file" == true ]]; then
         echo "Local version files updated."
    else
         # This case should only happen if nothing changed at all
         echo "No file updates were needed."
    fi
else
    # Notification was required but failed
    echo "Notification failed. Local files *not* updated to allow retry on next run." >&2
    exit 1 # Exit with error since desired state (notified+updated) wasn't reached
fi

echo "Script finished successfully."
exit 0

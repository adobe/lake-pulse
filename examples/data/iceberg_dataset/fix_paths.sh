#!/bin/bash
# Copyright 2025 Adobe. All rights reserved.
# This file is licensed to you under the Apache License,
# Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# or the MIT license (http://opensource.org/licenses/MIT),
# at your option.
#
# Unless required by applicable law or agreed to in writing,
# this software is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR REPRESENTATIONS OF ANY KIND, either express or
# implied. See the LICENSE-MIT and LICENSE-APACHE files for the
# specific language governing permissions and limitations under
# each license.

# This script fixes the hardcoded absolute paths in Iceberg metadata files.
# Iceberg requires absolute paths in metadata, so this script updates them
# to match the current machine's directory structure.
#
# Usage: ./fix_paths.sh
# Run this from the iceberg_dataset directory.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Placeholder used in the JSON files
PLACEHOLDER="<ICEBERG_TABLE_PATH>"

# The new absolute path for this machine
NEW_PATH_TRIPLE="file://$SCRIPT_DIR"    # file:///path format
NEW_PATH_SINGLE="file:$SCRIPT_DIR"       # file:/path format

echo "Updating Iceberg metadata paths to: $NEW_PATH_TRIPLE"

for metadata_file in metadata/*.metadata.json; do
    if [[ -f "$metadata_file" ]]; then
        echo "Processing $metadata_file..."

        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS sed requires empty string for -i
            # Replace placeholder with triple-slash format for "location" field
            sed -i '' "s|file://$PLACEHOLDER|$NEW_PATH_TRIPLE|g" "$metadata_file"
            # Replace placeholder with single-slash format for manifest-list and metadata-file
            sed -i '' "s|file:$PLACEHOLDER|$NEW_PATH_SINGLE|g" "$metadata_file"
        else
            # Linux sed
            sed -i "s|file://$PLACEHOLDER|$NEW_PATH_TRIPLE|g" "$metadata_file"
            sed -i "s|file:$PLACEHOLDER|$NEW_PATH_SINGLE|g" "$metadata_file"
        fi
    fi
done

echo "Done! Metadata paths updated."


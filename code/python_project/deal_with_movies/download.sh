FILE="/app/data/movie-100k/ml-100k.zip"
CHECKSUM_FILE="/tmp${FILE}.md5"
ZIP_URL="https://files.grouplens.org/datasets/movielens/ml-100k.zip"
MD5_URL="https://files.grouplens.org/datasets/movielens/ml-100k.zip.md5"
# Create the directory if it doesn't exist
mkdir -p "$(dirname "$FILE")"
# Function to download files
download_files() {
    echo "Downloading zip file..."
    wget -O "$FILE" "$ZIP_URL"
}
# Function to verify checksum
verify_checksum() {
    mkdir -p "$(dirname "$CHECKSUM_FILE")"
    wget -O "$CHECKSUM_FILE" "$MD5_URL"
    echo "Verifying checksum..."
    # Change to the directory containing the file
    cd "$(dirname "$FILE")" || exit 1
    # Extract the expected checksum
    EXPECTED_CHECKSUM=$(cut -d ' ' -f4 "$CHECKSUM_FILE")
    # Calculate the actual checksum
    ACTUAL_CHECKSUM=$(md5sum "$(basename "$FILE")" | cut -d ' ' -f1)
    if [ "$EXPECTED_CHECKSUM" == "$ACTUAL_CHECKSUM" ]; then
        echo "Checksum verification passed."
        return 0
    else
        echo "Checksum verification failed."
        return 1
    fi
}
# Main logic
if [ -f "$FILE" ]; then
    echo "File exists at $FILE"
    if verify_checksum; then
        echo "File is valid. No action needed."
        exit 0
    else
        echo "File is corrupted. Re-downloading..."
        rm -f "$FILE" "$CHECKSUM_FILE"
        download_files
        if verify_checksum; then
            echo "Re-downloaded file is valid."
            exit 0
        else
            echo "Checksum verification failed after re-download."
            exit 1
        fi
    fi
else
    echo "File does not exist. Downloading..."
    download_files
    if verify_checksum; then
        echo "Downloaded file is valid."
        rm -rf /app/data/movie-100k-u
        unzip /app/data/movie-100k/ml-100k.zip -d /app/data/movie-100k-u
        exit 0
    else
        echo "Checksum verification failed after download."
        exit 1
    fi
fi
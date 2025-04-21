# Use the latest Python image with a specific version
FROM spark:3.5.5-scala2.12-java17-python3-ubuntu

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt ./

# Ensure the correct permissions for the /home/spark directory
USER root
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark

# Switch back to the default user
USER spark

# Install Python dependencies globally
RUN pip3 install --no-cache-dir -r requirements.txt

# Set the working directory to the cloned project
WORKDIR /app/scrape_sentiments

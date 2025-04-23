# Base image
FROM spark:3.5.5-scala2.12-java17-python3-ubuntu

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Switch to root user to install packages
USER root

# Install OpenSSH server
RUN apt-get update && \
    apt-get install -y openssh-server && \
    mkdir /var/run/sshd

# Generate SSH host keys
RUN ssh-keygen -A

# Create .ssh directory for spark user
RUN mkdir -p /home/spark/.ssh && \
    chmod 700 /home/spark/.ssh && \
    chown -R spark:spark /home/spark/.ssh

# Copy the public key into authorized_keys
COPY ./spark/.ssh/authorized_keys /home/spark/.ssh/authorized_keys
COPY .bashrc /home/spark/.bashrc

# Set appropriate permissions
RUN chmod 600 /home/spark/.ssh/authorized_keys && \
    chown -R spark:spark /home/spark/.ssh && \
    chmod 644 /home/spark/.bashrc && \
    chown spark:spark /home/spark/.bashrc

# Configure SSH to allow key-based authentication
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin no/' /etc/ssh/sshd_config

# Expose SSH port
EXPOSE 22
# Copy the requirements file into the container
COPY requirements.txt ./

# Ensure the correct permissions for the /home/spark directory
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark

# Switch back to the default user
USER spark

# Install Python dependencies globally
RUN pip3 install --no-cache-dir -r requirements.txt

SHELL ["/bin/bash", "-c"]

# Set the working directory to the cloned project
WORKDIR /app/scrape_sentiments

USER root

# Start SSH service
CMD ["/usr/sbin/sshd", "-D"]
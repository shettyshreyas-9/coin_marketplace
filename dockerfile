# Use an official Python runtime as a parent image
FROM python:3.8-slim

WORKDIR /app

# Install Docker CLI and the latest stable version of docker-compose
RUN apt-get update && \
    apt-get install -y \
    docker.io \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
    -o /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose


# Copy the application files into the container
COPY . .

RUN pip install -r requirements.txt


# Expose the port that the app will run on
EXPOSE 8080

# Run the app.py script
CMD ["python", "app.py"]

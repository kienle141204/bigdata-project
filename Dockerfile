# 1. Use Debian Bullseye (Stable) which has openjdk-17 available in main repo
FROM python:3.10-slim-bullseye

# 2. Install System Dependencies: 
#    - OpenJDK 17 (For PySpark)
#    - Chromium & Driver (For Selenium Scraping)
#    - Utilities
# We add missing repositories if needed, but bullseye main usually has them.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    chromium \
    chromium-driver \
    procps \
    curl \
    unzip \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Set Environment Variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# Helpers for Selenium (optional, but good practice)
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# 3. Working Directory
WORKDIR /app

# 4. Install Python Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy Code
COPY . .

# 6. Default Command
CMD ["tail", "-f", "/dev/null"]

# Use a slim Python image for a smaller footprint
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Install system dependencies for Playwright
# This is a consolidated list of modern dependencies for Debian Trixie.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libxkbcommon0 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libgtk-3-0 \
    libdbus-glib-1-2 \
    libfontconfig1 \
    libfreetype6 \
    libx11-6 \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxext6 \
    libxi6 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    libappindicator3-1 \
    libgdk-pixbuf-2.0-0 \
    libenchant-2-2 \
    libicu-dev \
    libjpeg-dev \
    libvpx-dev \
    libwebp-dev \
    fonts-unifont \
    fonts-noto-color-emoji \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers, but do NOT install dependencies again
RUN playwright install chromium

# Copy the rest of the application code
COPY . .

# Set the entrypoint to run the FastAPI application with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
# Use a lightweight Python runtime
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Install the underlying OS dependencies Playwright requires
RUN apt-get update && apt-get install -y \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Copy your requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install the Playwright Chromium browser
RUN playwright install chromium

# Copy the rest of your project files
COPY . .

# Start the Gunicorn server and bind it to Render's required network port
CMD ["sh", "-c", "gunicorn app:app --bind 0.0.0.0:${PORT:-10000}"]

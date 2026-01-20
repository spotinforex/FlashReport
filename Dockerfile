# ---------- BASE IMAGE ----------
FROM python:3.11-slim

# ---------- SYSTEM SETUP ----------
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    build-essential \
    libc6-dev \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    && rm -rf /var/lib/apt/lists/*

# ---------- WORKDIR ----------
WORKDIR /flashreport

# ---------- COPY PROJECT FILES ----------
COPY __init__.py ./
COPY config.py ./
COPY flashreport_session.session ./
COPY main.py ./
COPY uv.lock ./
COPY pyproject.toml ./
COPY ./Algorithm/ ./Algorithm/
COPY ./Auth/ ./Auth/
COPY ./scrapper/ ./scrapper/

# ---------- INSTALL DEPENDENCIES ----------
# Install uv package (Uvicorn wrapper) and project dependencies
RUN pip install --no-cache-dir uv && uv sync

# Install Playwright and Chromium
RUN uv run playwright install chromium

# ---------- EXPOSE PORT ----------
EXPOSE 8000

# ---------- CMD ----------
CMD ["uv", "run", "uvicorn", "Auth.app:app", "--host", "0.0.0.0", "--port", "8000"]


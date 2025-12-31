FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

WORKDIR /realestate-scraper

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["xvfb-run", "-a", "python", "-u", "crexi_scraper.py"]
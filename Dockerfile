FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir '.[console]' && useradd --create-home --uid 10001 runtime && mkdir /data && chown runtime /data
COPY frontend/build/web ./frontend/build/web
USER 10001
ENV CONSOLE_HOST=0.0.0.0 CONSOLE_PORT=8765 CONSOLE_DATA_DIR=/data
EXPOSE 8765
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8765/healthz')"
CMD ["python", "-m", "ephemeraldb.console.app"]

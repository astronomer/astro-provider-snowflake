FROM python:3.9-slim-bullseye

COPY requirements.txt requirements-runner.txt /tmp/

RUN apt-get update && \
    apt-get install -y build-essential && \
    mkdir -m 777 /tmp/pip_cache && \
    /usr/local/bin/python3.9 -m pip install --upgrade pip && \
    /usr/local/bin/python3.9 -m pip install -r /tmp/requirements-runner.txt && \
    /usr/local/bin/python3.9 -m pip install --cache-dir=/tmp/pip_cache -r /tmp/requirements.txt && \
    /usr/local/bin/python3.9 -m pip install --upgrade pip && \
    /usr/local/bin/python3.9 -m pip install --cache-dir=/tmp/pip_cache -r /tmp/requirements.txt

WORKDIR /app
COPY src .

ENV PYTHONPATH="${PYTHONPATH}:/app"
ENV PYTHONUNBUFFERED=1

EXPOSE 8001/TCP

CMD ["python3.9", "-m", "gunicorn", "api:app", "--bind", "0.0.0.0:8001", "-w", "6", "-k", "uvicorn.workers.UvicornWorker", "--timeout", "0"]

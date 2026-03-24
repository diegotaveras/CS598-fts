FROM lmsysorg/sglang:v0.5.2-cu126

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["/bin/sh", "./node/start.sh"]
#CMD ["python3", "./agent/launch_server.py"]
FROM bitnami/spark:3.3

USER root

# Instalar Python y pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Instalar dependencias de Python
COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

COPY ./src /app
COPY ./data /app/data

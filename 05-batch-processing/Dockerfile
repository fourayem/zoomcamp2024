FROM docker.io/bitnami/spark:3.3

ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"  
    # you may need to update this in case the docker-compose version changes 
ENV PATH="${HOME}/.local/bin/:$PATH"

USER root
RUN apt-get update 
RUN apt-get install wget -qqq

# the rootless user ID
USER 1001    

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
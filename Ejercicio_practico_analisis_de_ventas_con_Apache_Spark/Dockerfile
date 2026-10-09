FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y python3-pip python3-dev build-essential vim nano bash-completion && \
    pip3 install jupyterlab pyspark numpy pandas ipython && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    echo ". /usr/share/bash-completion/bash_completion" >> /etc/bash.bashrc

RUN mkdir -p /home/spark/.local/share/jupyter/runtime && \
    mkdir -p /opt/spark-notebooks && \
    chown -R spark:spark /home/spark && \
    chown -R spark:spark /opt/spark-notebooks


ENV SHELL=/bin/bash

USER spark
WORKDIR /opt/spark-notebooks

# 🔥 THIS IS THE MAGIC
ENV JUPYTER_RUNTIME_DIR=/home/spark/jupyter_runtime
ENV JUPYTER_DATA_DIR=/home/spark/jupyter_data
ENV JUPYTER_CONFIG_DIR=/home/spark/jupyter_config

EXPOSE 8888 4040

CMD ["python3", "-m", "jupyterlab", "--ip=0.0.0.0", "--port=8888", "--no-browser","--NotebookApp.token=''"]
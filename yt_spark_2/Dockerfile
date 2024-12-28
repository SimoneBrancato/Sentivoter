FROM apache/spark:3.4.2

USER root

RUN pip install --no-cache-dir \
    tweetnlp \
    "huggingface-hub>=0.23.0" \
    "transformers>=4.41.0,<5.0.0" \
    "datasets<=2.10.1"
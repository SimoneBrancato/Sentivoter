FROM python:3.9-slim

# ENV PATH /usr/src/app/bin:$PATH
WORKDIR /usr/src/app
RUN apt update && apt install -y ffmpeg

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./ ./

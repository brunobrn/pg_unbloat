FROM python:3

WORKDIR /pg_unbloat

COPY /build/requirements.txt requirements.txt

COPY /pg_unbloat /pg_unbloat

RUN pip install --no-cache-dir -r requirements.txt
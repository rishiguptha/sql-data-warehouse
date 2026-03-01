# Pull the python base image

FROM python:3.12-slim

#Create working directory

WORKDIR /app


#COPY the files

COPY . .

#Install dependencies
RUN pip install --no-cache-dir uv
RUN uv sync

#Run the APP

CMD ["uv", "run", "scripts/run_pipeline.py"]
FROM python:3.12

# apptainer-in-apptainer
# copied from https://github.com/Observation-Management-Service/ewms-pilot/blob/v1.0.3/Dockerfile#L32
RUN apt update && \
    apt install -y wget && \
    cd /tmp && \
    wget https://github.com/apptainer/apptainer/releases/download/v1.3.3/apptainer_1.3.3_amd64.deb && \
    apt install -y ./apptainer_1.3.3_amd64.deb ; \

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

CMD ["python", "task.py"]

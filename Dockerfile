FROM python:3.13

COPY . .
RUN pip install -r requirements.txt

CMD ["python", "task.py"]

FROM python:3.12

COPY . .
RUN pip install -r requirements.txt

CMD ["python", "task.py"]

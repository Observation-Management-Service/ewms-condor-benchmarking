FROM python:3.13

COPY . .
CMD ["python", "task.py"]

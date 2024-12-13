FROM python:3.12

WORKDIR /app

COPY . .

RUN pip install pipenv

RUN pipenv install --system --deploy --ignore-pipfile

CMD ["python", "main.py"]

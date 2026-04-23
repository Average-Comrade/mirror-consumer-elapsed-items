FROM python:3.13 as builder


RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

ARG GITHUB_TOKEN

RUN git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"

COPY requirements.txt requirements.txt

# builder stage
RUN pip install --prefix=/install -r requirements.txt



FROM python:3.13 as runner

WORKDIR /APP

# runner stage
COPY --from=builder /install /usr/local

COPY main.py main.py

CMD ["python", "main.py"]
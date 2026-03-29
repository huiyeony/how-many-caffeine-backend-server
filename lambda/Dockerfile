FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ../tasks ./tasks
COPY ../core ./core
COPY ../rag ./rag
COPY handler.py ./

CMD ["handler.handler"]

FROM python:3.9 

WORKDIR /src
COPY . /src
ENV PYTHONPATH "${PYTHONPATH}:/src/modules"

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt


EXPOSE 8080

CMD ["python", "./main.py"] 
# this all still needs to be tested
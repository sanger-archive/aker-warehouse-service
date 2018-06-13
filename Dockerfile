# aker-events-consumer
FROM python:2.7.14

ENV PYTHONUNBUFFERED 1

# Install and setup project dependencies
RUN apt-get update && \
    apt-get install -y supervisor

RUN mkdir -p /var/log/supervisor

# Create the working directory
# https://docs.docker.com/engine/reference/builder/#workdir
WORKDIR /code

# Add the requirements file
ADD requirements.txt /code/requirements.txt

# Update pip
RUN pip install --upgrade pip

# Install packages required by project
RUN pip install -r requirements.txt

# Add the wait-for-it file to utils
ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /utils/wait-for-it.sh
RUN chmod u+x /utils/wait-for-it.sh

ADD consumer_supervisord.conf /etc/supervisor/conf.d/consumer_supervisord.conf

# Add all remaining contents to the image
ADD . /code

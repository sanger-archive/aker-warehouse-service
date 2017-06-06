Aker Events Consumer
===

Installation
---

Setup up a virtual environment using virtualenv:

`virtualenv venv`

and activate it

`source venv/bin/activate`

Install the project dependencies with pip

`pip install -r requirements.txt`


RabbitMQ
---

The easiest way to install RabbitMQ is using `brew`:

`brew install rabbitmq`

Once installed, you can run RabbitMQ using the `rabbitmq-server` command. This will also enable a web view at [http://localhost:15672](http://localhost:15672) which you can log into with the credentials `guest:guest`.

Once running, you will need to set up a topic exchange e.g. aker.events and a queue which binds to it (both of which can be done through the web view).

Running the Tests
---

The tests can be run using the [nose library](http://nose.readthedocs.io/).

`nosetests`

Database
---

You need to connect to a postgres database for this to work.
The connection details are in a local file called `[environment]_db.txt` (e.g. `development_db.txt`)

Load the `schema.sql` file into your database to set up the tables.

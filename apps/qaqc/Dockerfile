FROM python:3.11-slim as build

# Run environment setup script
COPY bash/ /bash/
COPY requirements.txt /.
RUN bash/setup_env.sh

# Run deployed environment setups
WORKDIR /app

COPY . .

CMD [ "./entrypoint.sh" ]

EXPOSE 5000

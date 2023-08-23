FROM nycplanning/build-base:latest

WORKDIR /app

COPY . .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT [ "python3", "-m", "streamlit", "run", "src/index.py" ]

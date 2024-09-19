FROM nycplanning/base:latest

WORKDIR /app

COPY . .

## assumes dcpy, pyproject.toml, constraints copied in
RUN python3 -m pip install ./dcpy -c constraints.txt
RUN python3 -m pip install -r requirements.txt -c constraints.txt

ENV DEPLOY=true

RUN rm -rf dcpy

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT [ "python3", "-m", "streamlit", "run", "src/index.py" ]

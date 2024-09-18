ARG base_tag=latest
FROM nycplanning/base:${base_tag}

COPY . .

RUN python3 -m pip install -r requirements.txt -c constraints.txt
RUN python3 -m pip install -r dcpy_requirements.txt -c constraints.txt

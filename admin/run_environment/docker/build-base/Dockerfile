ARG base_tag=latest
FROM nycplanning/base:${base_tag}

COPY . .

RUN uv pip install --system -r requirements.txt -c constraints.txt
RUN uv pip install --system -r dcpy_requirements.txt -c constraints.txt

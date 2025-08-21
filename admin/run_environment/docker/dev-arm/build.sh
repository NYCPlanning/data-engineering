cp ../../requirements.txt .
cp ../../../../pyproject.toml .

docker build . -t dcp-arm-dev:0.0.1

rm ./requirements.txt
rm ./pyproject.toml

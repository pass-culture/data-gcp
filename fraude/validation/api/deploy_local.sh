docker build -t pcvalidation --progress=plain .
docker run -it -p 8080:8080 pcvalidation
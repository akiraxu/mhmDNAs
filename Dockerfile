FROM node:16.20.2-buster
WORKDIR /app
COPY . .
RUN npm install
ENTRYPOINT bash ./run.sh
EXPOSE 8080

FROM node:20-alpine3.21 AS builder
EXPOSE 8080
WORKDIR /opt/server
ENV MONGO=true
COPY package.json /opt/server/
COPY server.js /opt/server/
RUN npm cache clean --force && npm install
CMD [ "node", "server.js" ]

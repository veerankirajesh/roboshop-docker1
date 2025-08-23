# FROM node:20.19.4-alpine3.22
# WORKDIR /opt/server
# COPY package.json .
# COPY *.js .
# RUN npm install
# ENV REDIS_HOST="redis" \
#     CATALOGUE_HOST="catalogue" \
#     CATALOGUE_PORT="8080"
# CMD ["node","server.js"]

FROM node:20-alpine3.21 AS builder
WORKDIR /opt/server
COPY package.json .
COPY *.js .
RUN npm install

FROM node:20-alpine3.21

# Create non-root user
RUN addgroup -S roboshop && adduser -S roboshop -G roboshop

# Set environment variables
ENV REDIS_HOST="redis" \
    CATALOGUE_HOST="catalogue" \
    CATALOGUE_PORT="8080"

WORKDIR /opt/server

# Copy files from builder stage and give ownership to roboshop user
COPY --from=builder --chown=roboshop:roboshop /opt/server /opt/server

# Switch to non-root user
USER roboshop

# Start the app
CMD ["node", "server.js"]


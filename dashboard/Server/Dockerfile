FROM maven:3.6.3-openjdk-14
WORKDIR /home/Server
COPY . .
RUN chmod +x *
EXPOSE 8080
EXPOSE 20207
CMD ["./run_server"]

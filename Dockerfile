FROM debian
WORKDIR /app
COPY target/release/minecraft-shawl . 
RUN apt update
RUN chmod +x minecraft-shawl
CMD ["./minecraft-shawl"]

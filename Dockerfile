FROM python:3.11.1-slim-bullseye

# Set working directory
WORKDIR /container

# Install pip and git git-lfs and Java runtime environment
RUN apt-get update && apt-get install -y python3-pip git git-lfs openjdk-11-jre

# Initialize Git LFS for the repository
RUN git lfs install

# Install libraries
RUN pip3 install pandas duckdb jupyter requests duckdb-engine python-dotenv==0.21.1

# connect Docker container to the host network
CMD ["--network", "bridge"]
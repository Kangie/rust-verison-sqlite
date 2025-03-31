# Use the official Rust image as the base image
FROM rust:latest

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Build the Rust application
RUN cargo build --release

# Expose the port the web app will run on
EXPOSE 8080

# Command to run the web app
CMD ["./target/release/rust_version_util"]

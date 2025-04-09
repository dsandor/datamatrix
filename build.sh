#!/bin/bash

# Build script for DataMatrix
# Handles cross-compilation for Linux AMD64 with DuckDB support

set -e

# Default values
OUTPUT_DIR="./dist"
VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")

# Parse command line arguments
TARGET_OS=${1:-"darwin"}
TARGET_ARCH=${2:-"arm64"}

# Create output directory
mkdir -p $OUTPUT_DIR

# Function to display usage information
usage() {
  echo "Usage: $0 [os] [arch]"
  echo ""
  echo "Build the DataMatrix application for different platforms"
  echo ""
  echo "Arguments:"
  echo "  os    Target operating system (darwin, linux)"
  echo "  arch  Target architecture (amd64, arm64)"
  echo ""
  echo "Examples:"
  echo "  $0                   # Build for current platform"
  echo "  $0 darwin arm64      # Build for macOS on Apple Silicon"
  echo "  $0 linux amd64       # Build for Linux on x86_64"
  echo ""
  exit 1
}

# Display help if requested
if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
  usage
fi

# Validate target OS
case $TARGET_OS in
  darwin|linux)
    # Valid OS
    ;;
  *)
    echo "Error: Unsupported OS: $TARGET_OS"
    usage
    ;;
esac

# Validate target architecture
case $TARGET_ARCH in
  amd64|arm64)
    # Valid architecture
    ;;
  *)
    echo "Error: Unsupported architecture: $TARGET_ARCH"
    usage
    ;;
esac

BINARY_NAME="datamatrix-${TARGET_OS}_${TARGET_ARCH}"

echo "Building DataMatrix for ${TARGET_OS}/${TARGET_ARCH}..."

# Check if we're building for the current platform
CURRENT_OS=$(uname -s | tr '[:upper:]' '[:lower:]')
if [ "$CURRENT_OS" == "darwin" ]; then
  CURRENT_OS="darwin"
elif [ "$CURRENT_OS" == "linux" ]; then
  CURRENT_OS="linux"
else
  CURRENT_OS="unknown"
fi

CURRENT_ARCH=$(uname -m)
if [ "$CURRENT_ARCH" == "x86_64" ]; then
  CURRENT_ARCH="amd64"
elif [ "$CURRENT_ARCH" == "arm64" ] || [ "$CURRENT_ARCH" == "aarch64" ]; then
  CURRENT_ARCH="arm64"
else
  CURRENT_ARCH="unknown"
fi

# For cross-compilation with CGO, we need special handling
if [ "$TARGET_OS" == "$CURRENT_OS" ] && [ "$TARGET_ARCH" == "$CURRENT_ARCH" ]; then
  # Native build - CGO is enabled by default
  echo "Building natively for ${TARGET_OS}/${TARGET_ARCH}"
  go build -o "${OUTPUT_DIR}/${BINARY_NAME}" .
else
  # Cross-compilation
  echo "Cross-compiling for ${TARGET_OS}/${TARGET_ARCH} from ${CURRENT_OS}/${CURRENT_ARCH}"
  
  # For Linux AMD64 target, use Docker to build with the correct environment
  if [ "$TARGET_OS" == "linux" ] && [ "$TARGET_ARCH" == "amd64" ]; then
    echo "Using Docker to build for Linux AMD64 with DuckDB support"
    
    # Create a temporary Dockerfile
    cat > Dockerfile.build << EOF
FROM golang:1.21

WORKDIR /app

# Install DuckDB dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy Go module files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o datamatrix-linux_amd64 .

CMD ["/bin/bash"]
EOF

    # Build using Docker
    docker build -f Dockerfile.build -t datamatrix-builder .
    
    # Extract the binary from the container
    docker create --name datamatrix-extract datamatrix-builder
    docker cp datamatrix-extract:/app/datamatrix-linux_amd64 "${OUTPUT_DIR}/${BINARY_NAME}"
    docker rm datamatrix-extract
    
    # Clean up
    rm Dockerfile.build
  else
    echo "Error: Cross-compilation for ${TARGET_OS}/${TARGET_ARCH} is not supported by this script."
    echo "Only Linux AMD64 is supported for cross-compilation with DuckDB."
    exit 1
  fi
fi

echo "Build complete: ${OUTPUT_DIR}/${BINARY_NAME}"

# Create archive
TAR_NAME="datamatrix-${TARGET_OS}_${TARGET_ARCH}.tar.gz"
echo "Creating tarball: ${OUTPUT_DIR}/${TAR_NAME}"
tar -czf "${OUTPUT_DIR}/${TAR_NAME}" -C "$OUTPUT_DIR" "${BINARY_NAME}" -C .. "README.md"

echo "Done!"

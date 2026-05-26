FROM gcr.io/oss-fuzz-base/base-builder:latest

# Install KEEL build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    ninja-build \
    libssl-dev \
    liburing-dev \
    && rm -rf /var/lib/apt/lists/*

# Clone the KEEL repository (OSS-Fuzz will override with the real HEAD)
RUN git clone --depth 1 https://github.com/virtlabs-io/keel $SRC/keel

WORKDIR $SRC/keel

COPY build.sh $SRC/

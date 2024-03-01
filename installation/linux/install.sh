#!/bin/bash

# Set the version you want to install
RES_VERSION="v0.2.0"

# Set the installation directory
INSTALL_DIR="/usr/local/bin"

# URL for the Resonate release tarball
RES_URL="https://github.com/resonatehq/resonate/releases/download/${RES_VERSION}/resonate-linux-amd64"

# Temporary directory for downloading
TMP_DIR=$(mktemp -d)

# Download Resonate binary
echo "Downloading Resonate ${RES_VERSION}..."
curl -L -o "${TMP_DIR}/resonate" "${RES_URL}"

# Install Resonate binary
echo "Installing Resonate to ${INSTALL_DIR}..."
sudo install "${TMP_DIR}/resonate" "${INSTALL_DIR}"

# Cleanup
echo "Cleaning up..."
rm -rf "${TMP_DIR}"

echo "Resonate ${RES_VERSION} has been installed!"

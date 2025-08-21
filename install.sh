#!/bin/bash

# Aegis Installation Script for SSL-Restricted Environments

echo "Installing Aegis requirements in SSL-restricted environment..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install with SSL bypass
echo "Installing requirements with SSL bypass..."
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org -r requirements.txt

if [ $? -eq 0 ]; then
    echo "✅ Installation successful!"
else
    echo "❌ Installation failed. Trying alternative method..."
    
    # Alternative: Set environment variables for SSL
    export PIP_TRUSTED_HOST="pypi.org files.pythonhosted.org pypi.python.org"
    export PIP_CERT=""
    export PYTHONHTTPSVERIFY=0
    
    pip install -r requirements.txt
fi

echo "Installation complete."
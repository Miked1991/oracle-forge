#!/bin/bash
# Setup script for Groq Llama injection tests

echo "========================================="
echo "Groq Llama Injection Test Setup"
echo "========================================="

# Install groq package
echo "Installing groq package..."
pip install groq python-dotenv

# Check if API key exists
if [ -z "$GROQ_API_KEY" ]; then
    echo ""
    echo "⚠️ GROQ_API_KEY not set in environment"
    echo ""
    echo "To get an API key:"
    echo "1. Go to https://console.groq.com"
    echo "2. Sign up / Log in"
    echo "3. Navigate to API Keys"
    echo "4. Create a new key"
    echo ""
    read -p "Enter your Groq API key (or press Enter to skip): " api_key
    
    if [ -n "$api_key" ]; then
        echo "GROQ_API_KEY=$api_key" >> .env
        echo "✅ API key saved to .env file"
        export GROQ_API_KEY=$api_key
    else
        echo "⚠️ No API key provided. Tests will use mock mode."
    fi
else
    echo "✅ GROQ_API_KEY found in environment"
fi

echo ""
echo "Setup complete! Run tests with:"
echo "  python kb/injection_test.py --mock     # Mock mode (no API key)"
echo "  python kb/injection_test.py            # Real Groq Llama"
echo "  python kb/injection_test.py --verbose  # Verbose output"
echo "  python kb/injection_test.py --model llama-3.3-70b-versatile"   
echo "  excution complete!"

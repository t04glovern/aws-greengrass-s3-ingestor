#!/bin/bash

# Custom build script for bundling Python dependencies and other files
# These commands must create a recipe and artifacts in the following folders
# within the component folder.

# - Recipe folder: greengrass-build/recipes
# - Artifacts folder: greengrass-build/artifacts/componentName/componentVersion

while [[ $# -gt 0 ]]; do
  case "$1" in
    --component-name=*)
      COMPONENT_NAME="${1#*=}"
      shift
      ;;
    --component-version=*)
      COMPONENT_VERSION="${1#*=}"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 3
      ;;
  esac
done

if [[ -z "$COMPONENT_NAME" || -z "$COMPONENT_VERSION" ]]; then
  cat <<-EOF
Usage: $0 --component-name=<name>
          --component-version=<version>
EOF
  exit 3
fi

# Check and delete directories and files if they exist
[ -d "./custom-build" ] && rm -rf ./custom-build
[ -d "./greengrass-build" ] && rm -rf ./greengrass-build
[ -f "bundle.zip" ] && rm -f bundle.zip

# Create necessary directories
mkdir -p "./greengrass-build/recipes"
mkdir -p "./greengrass-build/artifacts/$COMPONENT_NAME/$COMPONENT_VERSION"
mkdir -p "./custom-build"

# Copy recipe.yaml to the recipes directory
cp recipe.yaml ./greengrass-build/recipes

# Install Python dependencies to the custom-build directory
pip install -r requirements.txt -t ./custom-build

# Copy only main.py and the src folder to the custom-build directory
cp main.py ./custom-build
cp -r src ./custom-build/

# Archive the contents of ./custom-build folder directly into bundle.zip without the custom-build directory itself
(cd ./custom-build && zip -r ../bundle.zip *)

# Move bundle.zip to greengrass-build/artifacts/$COMPONENT_NAME/$COMPONENT_VERSION
mv bundle.zip ./greengrass-build/artifacts/$COMPONENT_NAME/$COMPONENT_VERSION/

#!/bin/bash

# Check if version is provided
if [ -z "$1" ]; then
  echo "Usage: ./create-release.sh <version>"
  echo "Example: ./create-release.sh 1.0.0"
  exit 1
fi

VERSION="$1"
TAG="v$VERSION"

# Confirm with user
echo "Creating release $TAG"
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Aborted"
  exit 1
fi

# Create and push tag
git tag -a "$TAG" -m "Release $TAG"
git push origin "$TAG"

echo "Tag $TAG pushed to GitHub."
echo "GitHub Actions workflow will automatically build and create the release."
echo "Check the progress at: https://github.com/$(git config --get remote.origin.url | sed -E 's/.*github.com[:\/](.*)(\.git)?/\1/')/actions"

#!/usr/bin/env bash

set -e

if [ -z "$1" ]; then
  exit 1
fi

ENTRY="$1"

if [ ! -f "$ENTRY" ]; then
  echo "Error: File '$ENTRY' does not exist."
  exit 1
fi

mkdir -p public

npx esbuild "$ENTRY" --bundle --format=esm --outfile=public/main.js

cat > public/index.html <<EOF
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>$1</title>
</head>
<body>
  <h1>Running $1</h1>
  <script type="module" src="./main.js"></script>
</body>
</html>
EOF

npx serve public

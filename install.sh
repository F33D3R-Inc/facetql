#!/usr/bin/env bash
# Installs the latest FacetQL release binary for macOS or Linux.
#   curl -fsSL https://raw.githubusercontent.com/FACETQL-LLC/facetql/main/install.sh | sh
#
# Not part of the CI build — this just needs to know the exact asset
# names release.yml publishes, so if those names change this script
# needs updating alongside it.
set -euo pipefail

REPO="FACETQL-LLC/facetql"
INSTALL_DIR="${ENOCHIAN_INSTALL_DIR:-/usr/local/bin}"

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Darwin)
    case "$arch" in
      arm64) asset="facetql-macos-arm64" ;;
      x86_64) asset="facetql-macos-x86_64" ;;
      *) echo "Unsupported macOS architecture: $arch" >&2; exit 1 ;;
    esac
    ;;
  Linux)
    case "$arch" in
      x86_64) asset="facetql-linux-x86_64" ;;
      *) echo "Unsupported Linux architecture: $arch — no prebuilt binary yet, build from source instead." >&2; exit 1 ;;
    esac
    ;;
  *)
    echo "This installer supports macOS and Linux. On Windows, download facetql-windows-x86_64.exe from:" >&2
    echo "  https://github.com/$REPO/releases/latest" >&2
    exit 1
    ;;
esac

url="https://github.com/$REPO/releases/latest/download/$asset"
echo "Downloading $asset from the latest release..."

tmp="$(mktemp)"
curl -fsSL "$url" -o "$tmp"
chmod +x "$tmp"

if [ -w "$INSTALL_DIR" ]; then
  mv "$tmp" "$INSTALL_DIR/facetql"
else
  echo "Need sudo to write to $INSTALL_DIR"
  sudo mv "$tmp" "$INSTALL_DIR/facetql"
fi

echo "Installed to $INSTALL_DIR/facetql"
echo "Run 'facetql init' then 'facetql start' to get going."

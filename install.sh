#!/bin/sh
# install.sh — установщик mcp-postgres
# Использование: curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/zeslava/mcp-postgres/main/install.sh | sh
set -e

REPO="zeslava/mcp-postgres"
BIN="mcp-postgres"

# ---------- определение платформы ----------
OS=$(uname -s)
ARCH=$(uname -m)

case "$OS" in
  Linux)
    case "$ARCH" in
      x86_64)          TARGET="x86_64-unknown-linux-gnu" ;;
      aarch64|arm64)   TARGET="aarch64-unknown-linux-gnu" ;;
      *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
    esac
    ;;
  Darwin)
    case "$ARCH" in
      arm64|aarch64)   TARGET="aarch64-apple-darwin" ;;
      *) echo "Unsupported architecture: $ARCH (only Apple Silicon is supported on macOS)" >&2; exit 1 ;;
    esac
    ;;
  *)
    echo "Unsupported OS: $OS. Use the pre-built .zip from the Releases page on Windows." >&2
    exit 1
    ;;
esac

# ---------- последняя версия ----------
echo "Fetching latest release..."
VERSION=$(curl --proto '=https' --tlsv1.2 -fsSL \
  "https://api.github.com/repos/${REPO}/releases/latest" \
  | grep '"tag_name"' \
  | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')

if [ -z "$VERSION" ]; then
  echo "Could not determine the latest release version." >&2
  exit 1
fi

echo "Installing ${BIN} ${VERSION} (${TARGET})..."

# ---------- загрузка ----------
FILENAME="${BIN}-${VERSION}-${TARGET}.tar.gz"
BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

curl --proto '=https' --tlsv1.2 -fsSL -o "$TMP/$FILENAME"        "${BASE_URL}/${FILENAME}"
curl --proto '=https' --tlsv1.2 -fsSL -o "$TMP/$FILENAME.sha256" "${BASE_URL}/${FILENAME}.sha256"

# ---------- проверка контрольной суммы ----------
cd "$TMP"
if command -v shasum >/dev/null 2>&1; then
  shasum -a 256 -c "$FILENAME.sha256"
elif command -v sha256sum >/dev/null 2>&1; then
  sha256sum -c "$FILENAME.sha256"
else
  echo "Warning: no sha256 tool found, skipping checksum verification." >&2
fi

# ---------- распаковка ----------
tar -xzf "$FILENAME"

# ---------- установка ----------
INSTALL_DIR=""
if [ -w /usr/local/bin ]; then
  INSTALL_DIR="/usr/local/bin"
elif command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
  INSTALL_DIR="/usr/local/bin"
  SUDO=sudo
else
  INSTALL_DIR="$HOME/.local/bin"
  mkdir -p "$INSTALL_DIR"
fi

${SUDO:-} install -m 755 "${BIN}-${VERSION}-${TARGET}/${BIN}" "${INSTALL_DIR}/${BIN}"

echo ""
echo "${BIN} ${VERSION} installed to ${INSTALL_DIR}/${BIN}"

# Подсказка, если ~/.local/bin не в PATH
case ":$PATH:" in
  *":$HOME/.local/bin:"*) ;;
  *)
    if [ "$INSTALL_DIR" = "$HOME/.local/bin" ]; then
      echo ""
      echo "Add ~/.local/bin to your PATH:"
      echo "  echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.profile"
      echo "  source ~/.profile"
    fi
    ;;
esac

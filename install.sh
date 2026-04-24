#!/bin/sh
# install.sh — установщик db-mcp
# Использование: curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/zeslava/db-mcp/main/install.sh | sh
set -e

REPO="zeslava/db-mcp"
BIN="db-mcp"

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
# По умолчанию ставим в ~/.local/bin (без sudo, без загрязнения системных путей).
# Чтобы поставить в системный каталог: INSTALL_DIR=/usr/local/bin sh install.sh
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

SUDO=""
if [ ! -d "$INSTALL_DIR" ]; then
  if ! mkdir -p "$INSTALL_DIR" 2>/dev/null; then
    if command -v sudo >/dev/null 2>&1; then
      SUDO=sudo
      $SUDO mkdir -p "$INSTALL_DIR"
    else
      echo "Cannot create ${INSTALL_DIR} and sudo is not available." >&2
      exit 1
    fi
  fi
fi

if [ -z "$SUDO" ] && [ ! -w "$INSTALL_DIR" ]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO=sudo
  else
    echo "${INSTALL_DIR} is not writable and sudo is not available." >&2
    exit 1
  fi
fi

${SUDO:-} install -m 755 "${BIN}-${VERSION}-${TARGET}/${BIN}" "${INSTALL_DIR}/${BIN}"

echo ""
echo "${BIN} ${VERSION} installed to ${INSTALL_DIR}/${BIN}"

# Подсказка, если каталог установки не в PATH
case ":$PATH:" in
  *":${INSTALL_DIR}:"*) ;;
  *)
    echo ""
    echo "Warning: ${INSTALL_DIR} is not in your PATH."
    if [ "$INSTALL_DIR" = "$HOME/.local/bin" ]; then
      echo "Add it with:"
      echo "  echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.profile"
      echo "  source ~/.profile"
    fi
    ;;
esac

#!/bin/bash
set -e

# ─────────────────────────────────────────────────────────────────────────────
# build_native.sh — Build the zerobus_ffi shared library for the .NET SDK
#
# This script builds the Rust FFI crate as a cdylib (shared library) and places
# the output in the dotnet/src/Zerobus/runtimes/<RID>/native/ directory, which
# is the standard .NET layout for native P/Invoke libraries.
#
# Usage:
#   ./build_native.sh            # Build for the current platform
#   ./build_native.sh --force    # Rebuild even if up to date
#
# The script is also invoked automatically by the .NET build (via MSBuild
# targets in Zerobus.csproj) so that `dotnet build` and `dotnet test` always
# have the native library available.
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FFI_DIR="$REPO_ROOT/zerobus-ffi"
RUNTIMES_DIR="$SCRIPT_DIR/src/Zerobus/runtimes"

FORCE=0
if [[ "${1:-}" == "--force" ]]; then
    FORCE=1
fi

# ── Detect platform ──────────────────────────────────────────────────────────

OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Darwin)
        case "$ARCH" in
            arm64)   RID="osx-arm64" ;;
            x86_64)  RID="osx-x64"   ;;
            *)       echo "✗ Unsupported macOS architecture: $ARCH"; exit 1 ;;
        esac
        LIB_NAME="libzerobus_ffi.dylib"
        ;;
    Linux)
        case "$ARCH" in
            aarch64|arm64) RID="linux-arm64" ;;
            x86_64)        RID="linux-x64"   ;;
            *)             echo "✗ Unsupported Linux architecture: $ARCH"; exit 1 ;;
        esac
        LIB_NAME="libzerobus_ffi.so"
        ;;
    MINGW*|MSYS*|CYGWIN*)
        RID="win-x64"
        LIB_NAME="zerobus_ffi.dll"
        ;;
    *)
        echo "✗ Unsupported OS: $OS"
        exit 1
        ;;
esac

TARGET_DIR="$RUNTIMES_DIR/$RID/native"
TARGET_PATH="$TARGET_DIR/$LIB_NAME"

echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│  Zerobus .NET — Native Library Build                            │"
echo "├──────────────────────────────────────────────────────────────────┤"
echo "│  Platform:  $OS / $ARCH"
echo "│  RID:       $RID"
echo "│  Library:   $LIB_NAME"
echo "│  Output:    $TARGET_DIR/"
echo "└──────────────────────────────────────────────────────────────────┘"

# ── Check for Rust toolchain ─────────────────────────────────────────────────

if ! command -v cargo &>/dev/null; then
    # Try sourcing the standard cargo env
    if [[ -f "$HOME/.cargo/env" ]]; then
        # shellcheck disable=SC1091
        source "$HOME/.cargo/env"
    fi
fi

if ! command -v cargo &>/dev/null; then
    echo ""
    echo "✗ cargo not found. Install the Rust toolchain:"
    echo "    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

# ── Check if rebuild is needed ───────────────────────────────────────────────

if [[ $FORCE -eq 0 && -f "$TARGET_PATH" ]]; then
    NEEDS_REBUILD=0

    # Check Cargo.toml
    if [[ "$FFI_DIR/Cargo.toml" -nt "$TARGET_PATH" ]]; then
        NEEDS_REBUILD=1
    fi

    # Check Rust source files
    if [[ $NEEDS_REBUILD -eq 0 ]]; then
        while IFS= read -r -d '' file; do
            if [[ "$file" -nt "$TARGET_PATH" ]]; then
                NEEDS_REBUILD=1
                break
            fi
        done < <(find "$FFI_DIR/src" -name "*.rs" -print0 2>/dev/null)
    fi

    if [[ $NEEDS_REBUILD -eq 0 ]]; then
        echo ""
        echo "✓ Native library is up to date: $TARGET_PATH"
        echo "  (use --force to rebuild anyway)"
        exit 0
    fi
fi

# ── Build ────────────────────────────────────────────────────────────────────

echo ""
echo "Building zerobus-ffi (release)..."
echo ""

cd "$FFI_DIR"

if [[ "$OS" == MINGW* || "$OS" == MSYS* || "$OS" == CYGWIN* ]]; then
    TARGET="x86_64-pc-windows-gnu"
    cargo build --release --target "$TARGET"
    CARGO_OUT="target/$TARGET/release/$LIB_NAME"
else
    cargo build --release
    CARGO_OUT="target/release/$LIB_NAME"
fi

# ── Copy to runtimes ─────────────────────────────────────────────────────────

if [[ ! -f "$CARGO_OUT" ]]; then
    echo ""
    echo "✗ Build succeeded but library not found at: $CARGO_OUT"
    echo "  Expected shared library ($LIB_NAME) — check Cargo.toml has:"
    echo '    crate-type = ["staticlib", "cdylib"]'
    exit 1
fi

mkdir -p "$TARGET_DIR"
cp "$CARGO_OUT" "$TARGET_PATH"

echo ""
echo "✓ Native library built and placed at:"
echo "    $TARGET_PATH"
echo ""
echo "  Size: $(du -h "$TARGET_PATH" | cut -f1)"
echo ""

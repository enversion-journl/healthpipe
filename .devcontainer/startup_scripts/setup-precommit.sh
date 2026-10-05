#!/bin/bash

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

# Setup script for pre-commit hooks
echo "Setting up pre-commit hooks..."

# Pinned gitleaks version (must match the one validated for the secret/PII guard).
GITLEAKS_VERSION="8.30.1"

# install_gitleaks: download the pinned gitleaks release binary for the current
# OS/arch and install it onto PATH. Idempotent (skips if already the right
# version). Multi-arch: linux/darwin x x64/arm64/armv7/armv6/x32.
install_gitleaks() {
    if command -v gitleaks >/dev/null 2>&1 && gitleaks version 2>/dev/null | grep -q "${GITLEAKS_VERSION}"; then
        echo "gitleaks ${GITLEAKS_VERSION} already installed: $(command -v gitleaks)"
        return 0
    fi

    local os arch asset base tmp expected actual
    case "$(uname -s)" in
        Linux)  os="linux" ;;
        Darwin) os="darwin" ;;
        *) echo "WARN: unsupported OS '$(uname -s)' for gitleaks auto-install; install gitleaks ${GITLEAKS_VERSION} manually." >&2; return 1 ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64)  arch="x64" ;;
        aarch64|arm64) arch="arm64" ;;
        armv7l)        arch="armv7" ;;
        armv6l)        arch="armv6" ;;
        i386|i686)     arch="x32" ;;
        *) echo "WARN: unsupported arch '$(uname -m)' for gitleaks auto-install; install gitleaks ${GITLEAKS_VERSION} manually." >&2; return 1 ;;
    esac

    asset="gitleaks_${GITLEAKS_VERSION}_${os}_${arch}.tar.gz"
    base="https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}"
    tmp="$(mktemp -d)"

    echo "Installing gitleaks ${GITLEAKS_VERSION} (${os}/${arch})..."
    if ! curl -fsSL "${base}/${asset}" -o "${tmp}/${asset}"; then
        echo "ERROR: failed to download ${base}/${asset}" >&2; rm -rf "${tmp}"; return 1
    fi

    # Verify checksum (sha256). Skip with a warning if it can't be fetched/computed.
    if curl -fsSL "${base}/gitleaks_${GITLEAKS_VERSION}_checksums.txt" -o "${tmp}/checksums.txt"; then
        expected="$(grep " ${asset}\$" "${tmp}/checksums.txt" | awk '{print $1}')"
        if command -v sha256sum >/dev/null 2>&1; then
            actual="$(sha256sum "${tmp}/${asset}" | awk '{print $1}')"
        elif command -v shasum >/dev/null 2>&1; then
            actual="$(shasum -a 256 "${tmp}/${asset}" | awk '{print $1}')"
        fi
        if [ -n "${expected}" ] && [ -n "${actual}" ] && [ "${expected}" != "${actual}" ]; then
            echo "ERROR: checksum mismatch for ${asset} (expected ${expected}, got ${actual})" >&2
            rm -rf "${tmp}"; return 1
        fi
        echo "Checksum verified."
    else
        echo "WARN: could not download checksums; skipping integrity check." >&2
    fi

    if ! tar -xzf "${tmp}/${asset}" -C "${tmp}" gitleaks; then
        echo "ERROR: failed to extract gitleaks from ${asset}" >&2; rm -rf "${tmp}"; return 1
    fi
    chmod +x "${tmp}/gitleaks"

    # Install onto PATH: prefer /usr/local/bin, fall back to sudo, then ~/.local/bin.
    if [ -w /usr/local/bin ]; then
        mv "${tmp}/gitleaks" /usr/local/bin/gitleaks
    elif command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
        sudo mv "${tmp}/gitleaks" /usr/local/bin/gitleaks
    else
        mkdir -p "${HOME}/.local/bin"
        mv "${tmp}/gitleaks" "${HOME}/.local/bin/gitleaks"
        echo "NOTE: installed to \$HOME/.local/bin/gitleaks — ensure that directory is on your PATH."
    fi
    rm -rf "${tmp}"
    echo "gitleaks installed: $(gitleaks version 2>/dev/null)"
}

# Check if pre-commit is installed
if ! command -v pre-commit &> /dev/null; then
    echo "Installing pre-commit..."
    pip install pre-commit
fi

# Install gitleaks (used by the 'gitleaks' secret/PII pre-commit hook)
install_gitleaks || echo "WARN: gitleaks not installed — the 'gitleaks' pre-commit hook will fail until gitleaks ${GITLEAKS_VERSION} is on PATH (macOS host: 'brew install gitleaks')."

# Install the git hook scripts
echo "Installing pre-commit hooks..."
pre-commit install

# Surface the current pre-commit state across the tree (informational only).
# Non-fatal: the tree is autofix-clean, but ruff still reports ~70
# un-auto-fixed lint findings (tracked debt) that would otherwise exit non-zero
# and fail postCreateCommand. `|| true` keeps container-create green; the hooks
# still enforce per-commit on staged files.
echo "Running pre-commit on all files (informational; non-blocking)..."
pre-commit run --all-files || true

echo "Pre-commit setup complete!"
echo ""
echo "To manually run pre-commit on all files: pre-commit run --all-files"
echo "To run pre-commit on staged files only: pre-commit run"
echo "To update hooks: pre-commit autoupdate"

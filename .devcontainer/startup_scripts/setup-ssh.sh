#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

# Seed /root/.ssh from the bind-mounted host keys (~/.ssh → /home/vscode/.ssh).
# Cursor/VS Code "Attach to Running Container" often runs as root, which does
# not see the vscode mount, so git over SSH fails with Permission denied
# (publickey) until these files exist in /root/.ssh.
set -euo pipefail

SRC=/home/vscode/.ssh
DEST=/root/.ssh

KEY=""
for candidate in id_ed25519 id_rsa id_ecdsa; do
  if [ -r "${SRC}/${candidate}" ]; then
    KEY="${candidate}"
    break
  fi
done

if [ -z "${KEY}" ]; then
  echo "setup-ssh: no readable private key in ${SRC}; skip"
  exit 0
fi

sudo mkdir -p "${DEST}"
sudo chmod 700 "${DEST}"
sudo cp -f "${SRC}/${KEY}" "${DEST}/${KEY}"
sudo chmod 600 "${DEST}/${KEY}"
if [ -f "${SRC}/${KEY}.pub" ]; then
  sudo cp -f "${SRC}/${KEY}.pub" "${DEST}/${KEY}.pub"
  sudo chmod 644 "${DEST}/${KEY}.pub"
fi
if [ -f "${SRC}/known_hosts" ]; then
  sudo cp -f "${SRC}/known_hosts" "${DEST}/known_hosts"
  sudo chmod 644 "${DEST}/known_hosts"
fi

# Do not copy the host ssh config: it Includes macOS-only paths (Colima).
sudo tee "${DEST}/config" >/dev/null <<EOF
Host gitlab.com
  HostName gitlab.com
  User git
  IdentityFile ~/.ssh/${KEY}
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
EOF
sudo chmod 600 "${DEST}/config"

echo "setup-ssh: installed ${KEY} into ${DEST}"

#!/usr/bin/env bash

set -euo pipefail

if ! command -v docker &>/dev/null; then
    sudo apt-get update
    sudo apt-get install ca-certificates curl
    sudo install -m 0755 -d /etc/apt/keyrings
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc

    # Add the repository to Apt sources:
    echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" |
        sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

if ! command -v make &>/dev/null; then
    sudo apt-get -y install make
fi

if ! command -v tmux &>/dev/null; then
    sudo apt-get -y install tmux
fi

# add metamoto user without a password
sudo useradd -m metamoto
sudo usermod -aG docker metamoto

if [[ ! -d "metamoto.gr" ]]; then
    git clone https://github.com/mhristof/metamoto.gr.git
fi

cat <<EOF >clickhouse/users.d/local.xml
EOF

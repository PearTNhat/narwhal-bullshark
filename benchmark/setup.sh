#!/bin/bash
set -e

# --- Hàm helper ---
check_and_install_apt() {
    PKG=$1
    if dpkg -s "$PKG" >/dev/null 2>&1; then
        echo "[✔] $PKG đã cài đặt"
    else
        echo "[...] Đang cài $PKG ..."
        sudo apt install -y "$PKG"
    fi
}

check_and_install_pip() {
    PKG=$1
    if ./venv/bin/pip show "$PKG" >/dev/null 2>&1; then
        echo "[✔] Python package $PKG đã có"
    else
        echo "[...] Cài Python package $PKG ..."
        ./venv/bin/pip install "$PKG"
    fi
}

# --- Kiểm tra Python 3.10 ---
if command -v python3.10 >/dev/null 2>&1; then
    echo "[✔] Python 3.10 đã có"
else
    echo "[...] Đang cài Python 3.10 ..."
    sudo apt install -y python3.10 python3.10-venv
fi

# --- Xoá venv cũ nếu có ---
if [ -d "venv" ]; then
    echo "[⚠] Xoá venv cũ ..."
    rm -rf venv
fi

# --- Tạo virtualenv mới ---
echo "[...] Tạo venv với python3.10"
python3.10 -m venv venv

# --- Active venv ---
source venv/bin/activate

# --- Cài đặt gói apt cần thiết ---
APT_PACKAGES=(
    python3.10-dev python3.10-venv python3.10-distutils
    build-essential pkg-config
    libfreetype6-dev libpng-dev
)

for pkg in "${APT_PACKAGES[@]}"; do
    check_and_install_apt "$pkg"
done

# --- Update pip ---
./venv/bin/pip install --upgrade pip setuptools wheel

# --- Cài đặt Python packages ---
PIP_PACKAGES=(
    numpy==1.23.5
    matplotlib==3.5.3
    fabric==2.6.0
    multiaddr
    toml
    requests
    six
    lexicon
    decorator
    pyyaml
)

for pkg in "${PIP_PACKAGES[@]}"; do
    check_and_install_pip "$pkg"
done

# --- Cài đặt requirements.txt nếu có ---
if [ -f "requirements.txt" ]; then
    echo "[...] Cài đặt requirements.txt ..."
    ./venv/bin/pip install -r requirements.txt
else
    echo "[!] Không có requirements.txt"
fi

# --- Chạy thử fab local ---
if [ -f "./venv/bin/fab" ]; then
    echo "[...] Chạy fab local"
    ./venv/bin/fab local
else
    echo "[!] Không tìm thấy fab trong venv"
fi

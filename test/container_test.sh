
# Assume running on latest fedora for now

dnf install coreutils which device-mapper cargo rustc systemd-devel clang-devel device-mapper-devel util-linux-core python3-toml python3-pudb git python3 -y || exit 1
cargo build || exit 1
export PATH=$PATH:`pwd`/target/debug

if [ ! -d dmtest-python ]; then
    git clone https://github.com/jthornber/dmtest-python/ || exit 1
fi

if [ ! -d linux ]; then
    git clone https://github.com/torvalds/linux.git || exit 1
fi

export DMTEST_KERNEL_SOURCE=`pwd`/linux || exit 1

cd dmtest-python || exit 1

export DMTEST_RESULT_SET=baseline
./dmtest health || exit 1



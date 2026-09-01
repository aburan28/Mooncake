#!/usr/bin/env bash
# Prepare a NIC for the Transfer Engine's zero-copy TCP data path.
#
#   setup_tcp_zerocopy.sh <interface> <rx-queues> [zero-copy ports...]
#
# Reserves the named receive queues for zero-copy traffic, steers the
# transport's data ports to them, keeps every other flow away via RSS, and
# raises the MTU. Requires CAP_NET_ADMIN. See
# docs/source/design/transfer-engine/tcp_zero_copy.md.
set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "usage: $0 <interface> <rx-queues,comma-separated> [ports...]" >&2
    exit 2
fi

IFACE=$1
QUEUES=$2
shift 2
PORTS=("$@")

echo "== $IFACE: enabling header/data split"
ethtool -G "$IFACE" tcp-data-split on || {
    echo "tcp-data-split is not supported by this driver; zero copy is unavailable" >&2
    exit 1
}

TOTAL=$(ethtool -l "$IFACE" | awk '/Combined:/ {n=$2} END {print n}')
FIRST_ZC=${QUEUES%%,*}
echo "== $IFACE: steering RSS to queues 0..$((FIRST_ZC - 1)) of $TOTAL"
ethtool -X "$IFACE" equal "$FIRST_ZC" || true

for port in "${PORTS[@]}"; do
    for queue in ${QUEUES//,/ }; do
        echo "== $IFACE: port $port -> rx queue $queue"
        ethtool -N "$IFACE" flow-type tcp4 dst-port "$port" action "$queue"
    done
done

echo "== $IFACE: MTU 9000"
ip link set dev "$IFACE" mtu 9000 || true

echo "done; set MC_TCP_ZC=1 MC_TCP_ZC_IFACE=$IFACE MC_TCP_ZC_RXQS=$QUEUES"

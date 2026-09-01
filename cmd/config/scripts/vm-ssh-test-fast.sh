#!/bin/bash
# vm-ssh-test-fast.sh — parallel SSH connectivity test with live stats
#
# Improvements over vm-ssh-test.sh:
#   - Single oc call to gather all pod data (not 1 per VM)
#   - Parallel SSH via xargs -P (up to MAX_PARALLEL)
#   - Only runs 'hostname' (dropped journalctl — not useful for connectivity)
#   - Per-round summary: total / ok / fail / elapsed time
#   - Tracks per-VM failure counts across rounds
#   - Failure log with structured fields

export KUBECONFIG="${KUBECONFIG:-/root/mno/kubeconfig}"
SSH_KEY="/tmp/kube-burner-cudn-density-2215641846/ssh"
FAILURE_LOG="ssh_failures_fast.log"
CUMULATIVE_LOG="ssh_cumulative_stats.log"
MAX_PARALLEL=30
CONNECT_TIMEOUT=10
ROUND=0

declare -A FAIL_COUNT

test_one() {
    local ns="$1" pod="$2" ip="$3"
    local out
    out=$(ssh -n \
        -o ConnectTimeout="$CONNECT_TIMEOUT" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR \
        -i "$SSH_KEY" "fedora@${ip}" hostname 2>&1)
    if [ $? -eq 0 ]; then
        echo "OK|${ns}|${pod}|${ip}|${out}"
    else
        echo "FAIL|${ns}|${pod}|${ip}|${out}"
    fi
}
export -f test_one
export SSH_KEY CONNECT_TIMEOUT

echo "Starting vm-ssh-test-fast.sh — $(date)"
echo "Parallelism: $MAX_PARALLEL, Timeout: ${CONNECT_TIMEOUT}s"

while true; do
    ROUND=$((ROUND + 1))
    START=$(date +%s)
    echo ""
    echo "===== Round $ROUND — $(date) ====="

    # Single oc call: get all pods as JSON, extract ns/pod/ip with jq
    VM_INPUT=$(mktemp)
    oc get pod -l kube-burner.io/job=cudn-density-workload -l kubevirt.io=virt-launcher -A -o json 2>/dev/null | \
    jq -r '
        .items[] |
        .metadata.namespace as $ns |
        .metadata.name as $pod |
        (
            .metadata.annotations["k8s.ovn.org/pod-networks"] // "" |
            if . == "" then null
            else
                fromjson | to_entries[] |
                select(.key | startswith($ns)) |
                .value.ip_address // "" |
                split("/")[0]
            end
        ) as $ip |
        select($ip != null and $ip != "") |
        "\($ns) \($pod) \($ip)"
    ' > "$VM_INPUT" 2>/dev/null

    TOTAL=$(wc -l < "$VM_INPUT")
    if [ "$TOTAL" -eq 0 ]; then
        echo "No VMs found, retrying in 60s..."
        rm -f "$VM_INPUT"
        sleep 60
        continue
    fi
    echo "Testing $TOTAL VMs..."

    # Run tests in parallel
    RESULTS_FILE=$(mktemp)
    cat "$VM_INPUT" | xargs -P "$MAX_PARALLEL" -I {} bash -c '
        read -r ns pod ip <<< "{}"
        test_one "$ns" "$pod" "$ip"
    ' >> "$RESULTS_FILE"
    rm -f "$VM_INPUT"

    # Tally results
    OK=0
    FAIL=0
    FAIL_LINES=()
    while IFS='|' read -r status ns pod ip detail; do
        if [ "$status" = "OK" ]; then
            OK=$((OK + 1))
        else
            FAIL=$((FAIL + 1))
            key="${ns}/${pod}"
            FAIL_COUNT["$key"]=$(( ${FAIL_COUNT["$key"]:-0} + 1 ))
            FAIL_LINES+=("  ${ns}/${pod} (${ip}): ${detail}")
            echo "[$(date)] round=$ROUND pod=$pod ip=$ip ns=$ns err=$detail" >> "$FAILURE_LOG"
        fi
    done < "$RESULTS_FILE"
    rm -f "$RESULTS_FILE"

    ELAPSED=$(( $(date +%s) - START ))
    PCT_OK=0
    [ "$TOTAL" -gt 0 ] && PCT_OK=$(( OK * 100 / TOTAL ))

    # Summary
    echo ""
    echo "--- Round $ROUND Summary ---"
    printf "Total: %d | OK: %d (%d%%) | FAIL: %d | Time: %ds\n" "$TOTAL" "$OK" "$PCT_OK" "$FAIL" "$ELAPSED"
    if [ $FAIL -gt 0 ]; then
        echo "Failed this round:"
        printf '%s\n' "${FAIL_LINES[@]}"
    fi

    # Cumulative stats: VMs that have failed more than once
    REPEAT_FAIL=0
    for k in "${!FAIL_COUNT[@]}"; do
        [ "${FAIL_COUNT[$k]}" -gt 1 ] && REPEAT_FAIL=$((REPEAT_FAIL + 1))
    done
    UNIQUE_FAIL=${#FAIL_COUNT[@]}
    echo ""
    echo "Cumulative: $UNIQUE_FAIL unique VMs failed | $REPEAT_FAIL with repeated failures"

    # Append to cumulative log
    echo "$(date +%s) round=$ROUND total=$TOTAL ok=$OK fail=$FAIL elapsed=${ELAPSED}s" >> "$CUMULATIVE_LOG"
    echo "----------------------------"

    # ==========================================
    # OVS FLOW ANALYSIS (sample one worker)
    # ==========================================
    echo ""
    echo "=========================================="
    echo "  OVS FLOW ANALYSIS"
    echo "=========================================="
    OVN_POD=$(oc get pods -n openshift-ovn-kubernetes -l app=ovnkube-node \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    OVN_NODE=$(oc get pod -n openshift-ovn-kubernetes "$OVN_POD" \
        -o jsonpath='{.spec.nodeName}' 2>/dev/null)
    if [ -n "$OVN_POD" ]; then
        echo "Sampling flows from node: $OVN_NODE ($OVN_POD)..."
        FLOWS_FILE=$(mktemp)
        oc exec -n openshift-ovn-kubernetes "$OVN_POD" -c ovnkube-controller \
            -- ovs-ofctl dump-flows br-int 2>/dev/null > "$FLOWS_FILE"
        FLOW_TOTAL=$(wc -l < "$FLOWS_FILE")

        pct() { awk "BEGIN{printf \"%.1f\", $1 * 100 / $2}"; }

        # --- Section 1: Flow type breakdown ---
        CUDN=$(grep -c '40\.' "$FLOWS_FILE")
        CONJUNCTION=$(grep -c 'conjunction' "$FLOWS_FILE")
        ICMPV6=$(grep -c 'icmp6' "$FLOWS_FILE")
        CT_STATE=$(grep -c 'ct_state' "$FLOWS_FILE")
        ARP=$(grep -c 'arp' "$FLOWS_FILE")
        DL_DST=$(grep -c 'dl_dst=0a:58' "$FLOWS_FILE")
        DROPS=$(grep -c 'actions=drop' "$FLOWS_FILE")

        fmt="%-45s %6d (%5s%%)\n"
        echo ""
        echo "1. FLOW TYPE BREAKDOWN"
        echo "------------------------------------------"
        printf "$fmt" "Total flows:" "$FLOW_TOTAL" "100.0"
        printf "$fmt" "CUDN Subnet Flows (40.x.0.0/16):" "$CUDN" "$(pct $CUDN $FLOW_TOTAL)"
        printf "$fmt" "Network Policy (conjunction):" "$CONJUNCTION" "$(pct $CONJUNCTION $FLOW_TOTAL)"
        printf "$fmt" "ICMPv6 Neighbor Discovery (ND):" "$ICMPV6" "$(pct $ICMPV6 $FLOW_TOTAL)"
        printf "$fmt" "Connection Tracking (ct_state):" "$CT_STATE" "$(pct $CT_STATE $FLOW_TOTAL)"
        printf "$fmt" "ARP flows:" "$ARP" "$(pct $ARP $FLOW_TOTAL)"
        printf "$fmt" "MAC address (dl_dst=0a:58) flows:" "$DL_DST" "$(pct $DL_DST $FLOW_TOTAL)"
        printf "$fmt" "Drop flows (actions=drop):" "$DROPS" "$(pct $DROPS $FLOW_TOTAL)"

        # --- Section 2: Drop analysis ---
        echo ""
        echo "2. DROP ANALYSIS"
        echo "------------------------------------------"
        TOTAL_PKTS_DROPPED=$(grep 'actions=drop' "$FLOWS_FILE" | \
            awk -F'n_packets=' '{split($2,a,","); s+=a[1]+0} END{print s}')
        ACTIVE_DROPS=$(grep 'actions=drop' "$FLOWS_FILE" | \
            awk -F'n_packets=' '{split($2,a,","); if(a[1]+0 > 0) c++} END{print c+0}')
        printf "%-45s %s\n" "Total packets dropped:" "$TOTAL_PKTS_DROPPED"
        printf "%-45s %d / %d\n" "Active drop flows (pkts > 0):" "$ACTIVE_DROPS" "$DROPS"

        echo ""
        echo "  Drop flows by table:"
        grep 'actions=drop' "$FLOWS_FILE" | \
            awk -F'table=' '{split($2,a,","); t=a[1]+0; count[t]++}
                 END{for(t in count) printf "    table %-4d  %6d drops\n",t,count[t]}' | sort -k2 -n

        # --- Section 3: Top drop flows by packet count ---
        echo ""
        echo "3. TOP 10 DROP FLOWS (by packet count)"
        echo "------------------------------------------"
        grep 'actions=drop' "$FLOWS_FILE" | \
            awk -F'n_packets=' '{split($2,a,","); pkts=a[1]+0; if(pkts>0) print pkts, $0}' | \
            sort -rn | head -10 | while read -r pkts line; do
                tbl=$(echo "$line" | grep -oP 'table=\K[0-9]+')
                meta=$(echo "$line" | grep -oP 'metadata=\K0x[0-9a-f]+' || echo "n/a")
                match=$(echo "$line" | grep -oP 'priority=\K[^,]+,[^ ]+ ' | head -1)
                printf "  %12s pkts | table=%-4s metadata=%-8s %s\n" \
                    "$(echo "$pkts" | sed ':a;s/\B[0-9]\{3\}\>/.&/;ta')" \
                    "$tbl" "$meta" "$match"
            done

        # --- Section 4: ACL drops (network policy) ---
        echo ""
        echo "4. ACL / NETWORK POLICY DROPS (tables 44, 60)"
        echo "------------------------------------------"
        ACL_ACTIVE=$(grep -E 'table=(44|60)' "$FLOWS_FILE" | grep 'actions=drop' | \
            awk -F'n_packets=' '{split($2,a,","); if(a[1]+0 > 0) c++} END{print c+0}')
        ACL_TOTAL_PKTS=$(grep -E 'table=(44|60)' "$FLOWS_FILE" | grep 'actions=drop' | \
            awk -F'n_packets=' '{split($2,a,","); s+=a[1]+0} END{print s}')
        printf "%-45s %d\n" "Active ACL drop rules:" "$ACL_ACTIVE"
        printf "%-45s %s\n" "Total ACL packets dropped:" "$ACL_TOTAL_PKTS"

        # --- Section 5: ARP drops ---
        echo ""
        echo "5. ARP DROPS (table 11 — can prevent connectivity)"
        echo "------------------------------------------"
        ARP_DROP_FLOWS=$(grep 'table=11' "$FLOWS_FILE" | grep 'arp' | grep -c 'actions=drop')
        ARP_DROP_PKTS=$(grep 'table=11' "$FLOWS_FILE" | grep 'arp' | grep 'actions=drop' | \
            awk -F'n_packets=' '{split($2,a,","); s+=a[1]+0} END{print s}')
        printf "%-45s %d\n" "ARP drop flows (table 11):" "$ARP_DROP_FLOWS"
        printf "%-45s %s\n" "ARP packets dropped:" "$ARP_DROP_PKTS"

        echo "=========================================="
        rm -f "$FLOWS_FILE"
    else
        echo "Could not find ovnkube-node pod — skipping flow analysis"
    fi

    sleep 60
done

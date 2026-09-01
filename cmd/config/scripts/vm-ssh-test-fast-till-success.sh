#!/bin/bash
# vm-ssh-test-fast-till-success.sh — SSH connectivity test with definitive pass/fail per VM
#
# For each VM:
#   - If SSH succeeds once -> marked SUCCESS permanently, never retried
#   - If SSH fails 10 times -> marked FAIL permanently, never retried
#   - Script exits automatically when every VM has a final verdict

export KUBECONFIG="${KUBECONFIG:-/root/mno/kubeconfig}"
SSH_KEY="/tmp/kube-burner-cudn-density-2215641846/ssh"
MAX_PARALLEL=5
CONNECT_TIMEOUT=10
MAX_RETRIES=10
ROUND=0

declare -A VM_STATUS      # "success" | "fail" | unset (pending)
declare -A VM_ATTEMPTS    # number of failed attempts so far
declare -A VM_IP          # ip for final report

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

echo "=========================================="
echo "  vm-ssh-test-fast-till-success.sh"
echo "=========================================="
echo "Started:       $(date)"
echo "Max retries:   $MAX_RETRIES per VM"
echo "Parallelism:   $MAX_PARALLEL"
echo "Timeout:       ${CONNECT_TIMEOUT}s"
echo "SSH key:       $SSH_KEY"
echo "=========================================="

while true; do
    ROUND=$((ROUND + 1))
    START=$(date +%s)

    # Discover all VM pods
    ALL_VMS=$(mktemp)
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
    ' > "$ALL_VMS" 2>/dev/null

    TOTAL_VMS=$(wc -l < "$ALL_VMS")
    if [ "$TOTAL_VMS" -eq 0 ]; then
        echo "No VMs found, retrying in 30s..."
        rm -f "$ALL_VMS"
        sleep 30
        continue
    fi

    # Record IPs for final report on first discovery
    while read -r ns pod ip; do
        key="${ns}/${pod}"
        VM_IP["$key"]="$ip"
    done < "$ALL_VMS"

    # Filter to only VMs that still need testing
    PENDING_VMS=$(mktemp)
    while read -r ns pod ip; do
        key="${ns}/${pod}"
        [ "${VM_STATUS[$key]}" = "success" ] && continue
        [ "${VM_STATUS[$key]}" = "fail" ] && continue
        echo "$ns $pod $ip"
    done < "$ALL_VMS" > "$PENDING_VMS"
    rm -f "$ALL_VMS"

    PENDING=$(wc -l < "$PENDING_VMS")
    DONE_SUCCESS=0
    DONE_FAIL=0
    for k in "${!VM_STATUS[@]}"; do
        [ "${VM_STATUS[$k]}" = "success" ] && DONE_SUCCESS=$((DONE_SUCCESS + 1))
        [ "${VM_STATUS[$k]}" = "fail" ] && DONE_FAIL=$((DONE_FAIL + 1))
    done

    if [ "$PENDING" -eq 0 ]; then
        rm -f "$PENDING_VMS"
        echo ""
        echo "All VMs have a final verdict. Exiting loop."
        break
    fi

    echo ""
    echo "===== Round $ROUND -- $(date) ====="
    printf "Total VMs: %d | Pending: %d | Done(pass): %d | Done(fail): %d\n" \
        "$TOTAL_VMS" "$PENDING" "$DONE_SUCCESS" "$DONE_FAIL"

    # Run SSH tests in parallel for pending VMs only
    RESULTS_FILE=$(mktemp)
    cat "$PENDING_VMS" | xargs -P "$MAX_PARALLEL" -I {} bash -c '
        read -r ns pod ip <<< "{}"
        test_one "$ns" "$pod" "$ip"
    ' >> "$RESULTS_FILE"
    rm -f "$PENDING_VMS"

    # Process results
    ROUND_OK=0
    ROUND_FAIL=0
    while IFS='|' read -r status ns pod ip detail; do
        key="${ns}/${pod}"
        if [ "$status" = "OK" ]; then
            VM_STATUS["$key"]="success"
            ROUND_OK=$((ROUND_OK + 1))
        else
            VM_ATTEMPTS["$key"]=$(( ${VM_ATTEMPTS["$key"]:-0} + 1 ))
            ROUND_FAIL=$((ROUND_FAIL + 1))
            if [ "${VM_ATTEMPTS[$key]}" -ge "$MAX_RETRIES" ]; then
                VM_STATUS["$key"]="fail"
            fi
        fi
    done < "$RESULTS_FILE"
    rm -f "$RESULTS_FILE"

    ELAPSED=$(( $(date +%s) - START ))
    printf "Round result: OK=%d  FAIL=%d  Time=%ds\n" "$ROUND_OK" "$ROUND_FAIL" "$ELAPSED"

    # Show newly finalized VMs this round
    for k in "${!VM_STATUS[@]}"; do
        if [ "${VM_STATUS[$k]}" = "fail" ] && [ "${VM_ATTEMPTS[$k]}" -eq "$MAX_RETRIES" ]; then
            echo "  GAVE UP: $k (${VM_IP[$k]}) after $MAX_RETRIES attempts"
        fi
    done

    sleep 10
done

# ==========================================
# FINAL REPORT
# ==========================================
echo ""
echo "=========================================="
echo "  FINAL REPORT"
echo "=========================================="
echo "  Date:   $(date)"
echo "  Rounds: $ROUND"
echo ""

SUCCESS_LIST=()
FAIL_LIST=()
for k in $(echo "${!VM_STATUS[@]}" | tr ' ' '\n' | sort); do
    if [ "${VM_STATUS[$k]}" = "success" ]; then
        SUCCESS_LIST+=("$k")
    else
        FAIL_LIST+=("$k")
    fi
done

echo "  SUCCESS: ${#SUCCESS_LIST[@]} VMs"
echo "  ------------------------------------------"
for vm in "${SUCCESS_LIST[@]}"; do
    attempts=${VM_ATTEMPTS[$vm]:-0}
    printf "    %-50s  %s  (attempt %d)\n" "$vm" "${VM_IP[$vm]}" "$((attempts + 1))"
done

echo ""
echo "  FAIL: ${#FAIL_LIST[@]} VMs (gave up after $MAX_RETRIES attempts)"
echo "  ------------------------------------------"
for vm in "${FAIL_LIST[@]}"; do
    printf "    %-50s  %s\n" "$vm" "${VM_IP[$vm]}"
done

echo ""
TOTAL_FINAL=$(( ${#SUCCESS_LIST[@]} + ${#FAIL_LIST[@]} ))
if [ "$TOTAL_FINAL" -gt 0 ]; then
    PCT=$(( ${#SUCCESS_LIST[@]} * 100 / TOTAL_FINAL ))
    echo "  Overall: ${#SUCCESS_LIST[@]}/${TOTAL_FINAL} passed ($PCT%)"
fi
echo "=========================================="

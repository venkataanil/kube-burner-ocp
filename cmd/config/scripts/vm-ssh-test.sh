FAILURE_LOG="ssh_failures.log"

while true; do
oc get pod -l kube-burner.io/job=cudn-density-workload -A -o jsonpath='{range .items[*]}{.metadata.namespace}{" "}{.metadata.name}{" "}{.metadata.annotations.kubevirt\.io/domain}{"\n"}{end}' | \
while read -r ns pod domain; do
    echo "Namespace: $ns | pod: $pod | Domain: $domain"
    network_json=$(oc get pod "$pod" -n "$ns" -o jsonpath='{.metadata.annotations.k8s\.ovn\.org/pod-networks}')
    specific_ip=$(echo "$network_json" | jq -r --arg ns "$ns" 'to_entries[] | select(.key | startswith($ns)) | .value.ip_address')
    echo "Specific IP: $specific_ip"
    ip=$(echo "$specific_ip" | awk -F/ '{print $1}')
    echo "Connecting to $ip"
    if ssh_output=$(ssh -n -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i /tmp/kube-burner-cudn-density-1438742237/ssh fedora@$ip hostname 2>&1); then
        echo "Success: $ssh_output"
    else
        # If the exit status is non-zero, it failed
        echo "[$(date)] FAILED to connect to Pod: $pod (IP: $ip, Namespace: $ns). Error: $ssh_output" >> "$FAILURE_LOG"
        echo "Connection failed. Logged to $FAILURE_LOG"
    fi
    if ssh_output=$(ssh -n -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i /tmp/kube-burner-cudn-density-1438742237/ssh fedora@$ip journalctl 2>&1); then
        echo "Success: $ssh_output"
    else
        # If the exit status is non-zero, it failed
        echo "[$(date)] FAILED to connect to Pod: $pod (IP: $ip, Namespace: $ns). Error: $ssh_output" >> "$FAILURE_LOG"
        echo "Connection failed. Logged to $FAILURE_LOG"
    fi
done
sleep 60
done

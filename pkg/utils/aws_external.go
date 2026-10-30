// Copyright 2025 The Kube-burner Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
)

// ExternalTargetInfra holds state for the external EC2 target instance
type ExternalTargetInfra struct {
	InstanceID string
	KeyName    string
	PrivateIP  string
	StartIP    string
	EndIP      string
	Region     string
	client     *ec2.Client
}

// NewExternalTargetInfra creates an AWS EC2 client for the given region
func NewExternalTargetInfra(region string) (*ExternalTargetInfra, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return &ExternalTargetInfra{
		Region: region,
		client: ec2.NewFromConfig(cfg),
	}, nil
}

// CreateInstance creates an external EC2 instance in the same VPC/subnet/SG/AZ as the given worker node
func (e *ExternalTargetInfra) CreateInstance(ctx context.Context, workerNodeDNS, instanceType string, maxTargets int) error {
	log.Infof("Looking up worker node %s in AWS", workerNodeDNS)

	// Get worker instance metadata
	desc, err := e.client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("private-dns-name"), Values: []string{workerNodeDNS}},
		},
	})
	if err != nil || len(desc.Reservations) == 0 || len(desc.Reservations[0].Instances) == 0 {
		return fmt.Errorf("worker node %s not found in AWS: %w", workerNodeDNS, err)
	}
	worker := desc.Reservations[0].Instances[0]
	vpcID := aws.ToString(worker.VpcId)
	subnetID := aws.ToString(worker.SubnetId)
	az := aws.ToString(worker.Placement.AvailabilityZone)
	amiID := aws.ToString(worker.ImageId)

	// Find node security group
	var sgID string
	for _, sg := range worker.SecurityGroups {
		if strings.Contains(aws.ToString(sg.GroupName), "node") {
			sgID = aws.ToString(sg.GroupId)
			break
		}
	}
	if sgID == "" && len(worker.SecurityGroups) > 0 {
		sgID = aws.ToString(worker.SecurityGroups[0].GroupId)
	}
	log.Infof("Using VPC=%s Subnet=%s SG=%s AZ=%s AMI=%s", vpcID, subnetID, sgID, az, amiID)

	// Find 'maxTargets' consecutive unused IPs in subnet
	startIP, endIP, err := e.findFreeIPs(ctx, subnetID, maxTargets)
	if err != nil {
		return fmt.Errorf("find free IPs: %w", err)
	}
	e.StartIP = startIP
	e.EndIP = endIP
	log.Infof("External target IP range: %s - %s", startIP, endIP)

	// Create fresh key pair
	keyName := fmt.Sprintf("midterm-b-%d", time.Now().Unix())
	keyOut, err := e.client.CreateKeyPair(ctx, &ec2.CreateKeyPairInput{
		KeyName: aws.String(keyName),
	})
	if err != nil {
		return fmt.Errorf("create key pair: %w", err)
	}
	e.KeyName = keyName
	sshPrivKey := aws.ToString(keyOut.KeyMaterial)
	log.Infof("Created key pair %s", keyName)

	// Launch instance
	runOut, err := e.client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String(amiID),
		InstanceType: ec2types.InstanceType(instanceType),
		KeyName:      aws.String(keyName),
		SubnetId:     aws.String(subnetID),
		SecurityGroupIds: []string{sgID},
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		Placement:    &ec2types.Placement{AvailabilityZone: aws.String(az)},
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeInstance,
				Tags: []ec2types.Tag{{Key: aws.String("Name"), Value: aws.String("midterm-b-external")}},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("run instance: %w", err)
	}
	e.InstanceID = aws.ToString(runOut.Instances[0].InstanceId)
	log.Infof("Launched instance %s, waiting for running state...", e.InstanceID)

	// Wait for instance running
	waiter := ec2.NewInstanceRunningWaiter(e.client)
	if err := waiter.Wait(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{e.InstanceID},
	}, 5*time.Minute); err != nil {
		return fmt.Errorf("wait instance running: %w", err)
	}
	time.Sleep(30 * time.Second)

	// Get private IP
	descOut, err := e.client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{e.InstanceID},
	})
	if err != nil {
		return fmt.Errorf("describe instance: %w", err)
	}
	e.PrivateIP = aws.ToString(descOut.Reservations[0].Instances[0].PrivateIpAddress)
	eniID := aws.ToString(descOut.Reservations[0].Instances[0].NetworkInterfaces[0].NetworkInterfaceId)
	log.Infof("Instance running at %s, ENI=%s", e.PrivateIP, eniID)

	// Assign secondary IPs
	secondaryIPs := ipRange(startIP, endIP)
	if _, err := e.client.AssignPrivateIpAddresses(ctx, &ec2.AssignPrivateIpAddressesInput{
		NetworkInterfaceId: aws.String(eniID),
		PrivateIpAddresses: secondaryIPs,
	}); err != nil {
		return fmt.Errorf("assign private IPs: %w", err)
	}
	log.Infof("Assigned %d secondary IPs (%s-%s) to %s", len(secondaryIPs), startIP, endIP, eniID)

	// Configure ipvlan targets via SSH from worker node
	if err := e.configureIpvlans(workerNodeDNS, sshPrivKey, maxTargets, startIP, endIP); err != nil {
		return fmt.Errorf("configure ipvlans: %w", err)
	}

	return nil
}

// configureIpvlans SSHes to external instance from a worker node and sets up ipvlan targets
func (e *ExternalTargetInfra) configureIpvlans(workerDNS, sshPrivKey string, maxTargets int, startIP, endIP string) error {
	log.Infof("Configuring ipvlan targets on %s via SSH", e.PrivateIP)

	startIPParts := strings.Split(startIP, ".")
	endIPParts := strings.Split(endIP, ".")
	thirdOctet := startIPParts[2]
	startFourth := startIPParts[3]
	endFourth := endIPParts[3]

	// Download ipvlan script
	ipvlanScriptURL := "https://gist.githubusercontent.com/venkataanil/20955f74c0364bf8a530f3c6e1cad6a2/raw/gistfile1.txt"

	// Build remote setup script
	remoteScript := fmt.Sprintf(`#!/bin/bash
set -e
# Find primary network interface
IFACE=$(ip route | grep default | awk '{print $5}' | head -1)
echo "Interface: $IFACE"

# Download ipvlan script
curl -sL -o /tmp/create-ipvlan-targets.sh '%s'

# Fix third/fourth octets
sed -i 's/for third in [0-9]*/for third in %s/' /tmp/create-ipvlan-targets.sh
sed -i 's/for fourth in $(seq [0-9]* [0-9]*)/for fourth in $(seq %s %s)/' /tmp/create-ipvlan-targets.sh

# Wait for all secondary IPs to appear (up to 10 min)
EXPECTED=%d
for attempt in $(seq 1 60); do
  CURRENT=$(ip a show $IFACE | grep "inet " | wc -l)
  echo "Attempt $attempt: $CURRENT/$EXPECTED IPs on $IFACE"
  [ "$CURRENT" -ge "$EXPECTED" ] && break
  sleep 10
done

# Remove secondary IPs (AWS auto-added them)
for ip in $(ip a show $IFACE | grep "inet " | grep secondary | awk '{print $2}'); do
  ip a d $ip dev $IFACE 2>/dev/null || true
done
echo "Secondary IPs removed"

# Run ipvlan setup
chmod +x /tmp/create-ipvlan-targets.sh
UPLINK=$IFACE MAX_TARGETS=%d bash /tmp/create-ipvlan-targets.sh
echo "ipvlan setup complete"
`, ipvlanScriptURL, thirdOctet, startFourth, endFourth, maxTargets+1, maxTargets)

	// Parse PEM private key
	signer, err := ssh.ParsePrivateKey([]byte(sshPrivKey))
	if err != nil {
		return fmt.Errorf("parse private key: %w", err)
	}

	sshCfg := &ssh.ClientConfig{
		User:            "ec2-user",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // internal VPC — acceptable
		Timeout:         30 * time.Second,
	}

	// Connect directly to external instance (same VPC as kube-burner pod via cluster network)
	sshConn, err := ssh.Dial("tcp", fmt.Sprintf("%s:22", e.PrivateIP), sshCfg)
	if err != nil {
		return fmt.Errorf("SSH dial %s: %w", e.PrivateIP, err)
	}
	defer sshConn.Close()

	session, err := sshConn.NewSession()
	if err != nil {
		return fmt.Errorf("SSH new session: %w", err)
	}
	defer session.Close()

	var outBuf bytes.Buffer
	session.Stdout = &outBuf
	session.Stderr = &outBuf

	log.Infof("SSH connected to %s, running ipvlan setup...", e.PrivateIP)
	if err := session.Run(remoteScript); err != nil {
		return fmt.Errorf("ipvlan setup failed: %w\nOutput: %s", err, outBuf.String())
	}
	log.Infof("ipvlan setup output:\n%s", outBuf.String())
	return nil
}

// Destroy terminates the external instance and deletes the key pair
func (e *ExternalTargetInfra) Destroy(ctx context.Context) {
	if e.InstanceID != "" {
		log.Infof("Terminating external instance %s", e.InstanceID)
		if _, err := e.client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
			InstanceIds: []string{e.InstanceID},
		}); err != nil {
			log.Errorf("Failed to terminate instance %s: %v", e.InstanceID, err)
		}
	}
	if e.KeyName != "" {
		log.Infof("Deleting key pair %s", e.KeyName)
		if _, err := e.client.DeleteKeyPair(ctx, &ec2.DeleteKeyPairInput{
			KeyName: aws.String(e.KeyName),
		}); err != nil {
			log.Errorf("Failed to delete key pair %s: %v", e.KeyName, err)
		}
	}
}

// findFreeIPs finds 'count' consecutive unused IPs in the given subnet
func (e *ExternalTargetInfra) findFreeIPs(ctx context.Context, subnetID string, count int) (string, string, error) {
	// Get subnet CIDR
	subnetOut, err := e.client.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		SubnetIds: []string{subnetID},
	})
	if err != nil || len(subnetOut.Subnets) == 0 {
		return "", "", fmt.Errorf("describe subnet %s: %w", subnetID, err)
	}
	cidr := aws.ToString(subnetOut.Subnets[0].CidrBlock)

	// Get all used IPs in subnet (AWS ENIs)
	enisOut, err := e.client.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("subnet-id"), Values: []string{subnetID}},
		},
	})
	if err != nil {
		return "", "", fmt.Errorf("describe ENIs in subnet %s: %w", subnetID, err)
	}

	used := make(map[string]bool)
	for _, eni := range enisOut.NetworkInterfaces {
		for _, pip := range eni.PrivateIpAddresses {
			used[aws.ToString(pip.PrivateIpAddress)] = true
		}
	}

	// Walk subnet hosts, collect first 'count' consecutive free IPs
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", "", fmt.Errorf("parse CIDR %s: %w", cidr, err)
	}

	var freeIPs []string
	start := ipNet.IP.Mask(ipNet.Mask)
	ipInt := new(big.Int).SetBytes(start.To4())
	ones, bits := ipNet.Mask.Size()
	hostCount := (1 << (bits - ones)) - 2 // exclude network + broadcast

	for i := 1; i <= hostCount; i++ {
		ipInt.Add(ipInt, big.NewInt(1))
		b := ipInt.Bytes()
		if len(b) < 4 {
			b = append(make([]byte, 4-len(b)), b...)
		}
		ipStr := fmt.Sprintf("%d.%d.%d.%d", b[0], b[1], b[2], b[3])
		if !used[ipStr] {
			freeIPs = append(freeIPs, ipStr)
			if len(freeIPs) == count {
				break
			}
		} else {
			// Not consecutive — reset
			freeIPs = freeIPs[:0]
		}
	}

	if len(freeIPs) < count {
		return "", "", fmt.Errorf("not enough free IPs in subnet %s (need %d, found %d)", subnetID, count, len(freeIPs))
	}

	// Sort to ensure order
	sort.Slice(freeIPs, func(i, j int) bool {
		a, _ := netip.ParseAddr(freeIPs[i])
		b, _ := netip.ParseAddr(freeIPs[j])
		return a.Less(b)
	})

	return freeIPs[0], freeIPs[len(freeIPs)-1], nil
}

// ipRange returns all IPs between startIP and endIP inclusive
func ipRange(startIP, endIP string) []string {
	start := new(big.Int).SetBytes(net.ParseIP(startIP).To4())
	end := new(big.Int).SetBytes(net.ParseIP(endIP).To4())
	var ips []string
	for i := new(big.Int).Set(start); i.Cmp(end) <= 0; i.Add(i, big.NewInt(1)) {
		b := i.Bytes()
		if len(b) < 4 {
			b = append(make([]byte, 4-len(b)), b...)
		}
		ips = append(ips, fmt.Sprintf("%d.%d.%d.%d", b[0], b[1], b[2], b[3]))
	}
	return ips
}

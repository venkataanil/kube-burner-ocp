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
	"context"
	"encoding/base64"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	log "github.com/sirupsen/logrus"
)

// ExternalTargetInfra holds state for the external EC2 target instance
type ExternalTargetInfra struct {
	InstanceID     string
	KeyName        string
	PrivateIP      string
	StartIP        string
	EndIP          string
	Region         string
	InstanceIDFile string // path to write instance ID for teardown
	client         *ec2.Client
}

// NewExternalTargetInfra creates an AWS EC2 client. Region auto-detected from SDK defaults.
func NewExternalTargetInfra(region string) (*ExternalTargetInfra, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return &ExternalTargetInfra{
		Region: region,
		client: ec2.NewFromConfig(cfg),
	}, nil
}

// CreateInstance creates an external EC2 instance in the same VPC/subnet/SG/AZ as the given worker node.
// ipvlan targets are configured via EC2 user-data at boot — no SSH from Prow pod required.
func (e *ExternalTargetInfra) CreateInstance(ctx context.Context, workerNodeDNS, instanceType string, maxTargets int) error {
	log.Infof("Looking up worker node %s in AWS", workerNodeDNS)

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

	// Auto-detect region from AZ (strip last char: "us-west-2a" → "us-west-2")
	if e.Region == "" && len(az) > 1 {
		detectedRegion := az[:len(az)-1]
		log.Infof("Auto-detected region %s from AZ %s", detectedRegion, az)
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(detectedRegion))
		if err != nil {
			return fmt.Errorf("reload AWS config with detected region: %w", err)
		}
		e.client = ec2.NewFromConfig(cfg)
		e.Region = detectedRegion
	}

	// Find worker node security group
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
	// Use Amazon Linux 2023 AMI — supports bash user-data and ec2-user (RHCOS uses Ignition, not cloud-init)
	al2023AMI, err := e.getAL2023AMI(ctx)
	if err != nil {
		log.Warnf("Could not find AL2023 AMI, falling back to worker AMI %s: %v", amiID, err)
	} else {
		amiID = al2023AMI
	}
	log.Infof("VPC=%s Subnet=%s SG=%s AZ=%s AMI=%s", vpcID, subnetID, sgID, az, amiID)

	// Find maxTargets consecutive unused IPs in subnet
	startIP, endIP, err := e.findFreeIPs(ctx, subnetID, maxTargets)
	if err != nil {
		return fmt.Errorf("find free IPs: %w", err)
	}
	e.StartIP = startIP
	e.EndIP = endIP
	log.Infof("External target IP range: %s - %s", startIP, endIP)

	// Build user-data script — configures ipvlan targets at boot (no SSH needed)
	thirdOctet := strings.Split(startIP, ".")[2]
	startFourth := strings.Split(startIP, ".")[3]
	endFourth := strings.Split(endIP, ".")[3]
	expectedCount := maxTargets + 1 // primary + secondary

	userDataScript := fmt.Sprintf(`#!/bin/bash
set -e
exec > /var/log/ipvlan-setup.log 2>&1

# Download ipvlan script
curl -sL -o /tmp/create-ipvlan-targets.sh \
  'https://gist.githubusercontent.com/venkataanil/20955f74c0364bf8a530f3c6e1cad6a2/raw/gistfile1.txt'

# Patch for our IP range
sed -i 's/for third in [0-9]*/for third in %s/' /tmp/create-ipvlan-targets.sh
sed -i 's/for fourth in $(seq [0-9]* [0-9]*)/for fourth in $(seq %s %s)/' /tmp/create-ipvlan-targets.sh

# Find default interface
IFACE=$(ip route | grep default | awk '{print $5}' | head -1)
echo "Interface: $IFACE"

# Wait for all secondary IPs to appear (up to 10 minutes)
for attempt in $(seq 1 60); do
  CURRENT=$(ip a show $IFACE | grep 'inet ' | wc -l)
  echo "Attempt $attempt: $CURRENT/%d IPs on $IFACE"
  [ "$CURRENT" -ge "%d" ] && break
  sleep 10
done

# Remove AWS auto-added secondary IPs
ip a show $IFACE | grep 'inet ' | grep secondary | awk '{print $2}' | while read ip; do
  ip a d "$ip" dev $IFACE 2>/dev/null || true
done
echo "Secondary IPs removed"

# Run ipvlan setup
chmod +x /tmp/create-ipvlan-targets.sh
UPLINK=$IFACE MAX_TARGETS=%d bash /tmp/create-ipvlan-targets.sh

echo "IPVLAN_SETUP_COMPLETE" > /tmp/ipvlan-done
`, thirdOctet, startFourth, endFourth, expectedCount, expectedCount, maxTargets)

	userDataEncoded := base64.StdEncoding.EncodeToString([]byte(userDataScript))

	// Launch instance with user-data
	runOut, err := e.client.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:          aws.String(amiID),
		InstanceType:     ec2types.InstanceType(instanceType),
		SubnetId:         aws.String(subnetID),
		SecurityGroupIds: []string{sgID},
		MinCount:         aws.Int32(1),
		MaxCount:         aws.Int32(1),
		Placement:        &ec2types.Placement{AvailabilityZone: aws.String(az)},
		UserData:         aws.String(userDataEncoded),
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeInstance,
				Tags:         []ec2types.Tag{{Key: aws.String("Name"), Value: aws.String("midterm-b-external")}},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("run instance: %w", err)
	}
	e.InstanceID = aws.ToString(runOut.Instances[0].InstanceId)
	log.Infof("Launched instance %s, waiting for running state...", e.InstanceID)
	// B4: Write instance ID immediately after launch (before post-launch steps that could fail)
	// so teardown can clean up even if setup fails mid-way
	if e.InstanceIDFile != "" {
		if werr := os.WriteFile(e.InstanceIDFile, []byte(e.InstanceID), 0644); werr != nil {
			log.Warnf("Failed to write instance ID to %s: %v", e.InstanceIDFile, werr)
		}
	}

	waiter := ec2.NewInstanceRunningWaiter(e.client)
	if err := waiter.Wait(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{e.InstanceID},
	}, 5*time.Minute); err != nil {
		return fmt.Errorf("wait instance running: %w", err)
	}

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
	log.Infof("Assigned %d secondary IPs (%s-%s)", len(secondaryIPs), startIP, endIP)

	// Wait for user-data to complete (ipvlan setup runs at boot, takes ~5-8 min)
	log.Infof("Waiting 12 minutes for user-data ipvlan setup to complete...")
	time.Sleep(12 * time.Minute)
	log.Infof("External instance %s ready with %d ipvlan targets (%s-%s)", e.PrivateIP, maxTargets, startIP, endIP)

	return nil
}

// Destroy terminates the external instance and waits for full termination
func (e *ExternalTargetInfra) Destroy(ctx context.Context) {
	if e.InstanceID != "" {
		log.Infof("Terminating external instance %s", e.InstanceID)
		if _, err := e.client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
			InstanceIds: []string{e.InstanceID},
		}); err != nil {
			log.Errorf("Failed to terminate instance %s: %v", e.InstanceID, err)
			return
		}
		// Wait for full termination so VPC cleanup can proceed
		log.Infof("Waiting for instance %s to terminate...", e.InstanceID)
		waiter := ec2.NewInstanceTerminatedWaiter(e.client)
		if err := waiter.Wait(ctx, &ec2.DescribeInstancesInput{
			InstanceIds: []string{e.InstanceID},
		}, 5*time.Minute); err != nil {
			log.Errorf("Instance %s did not terminate in time: %v", e.InstanceID, err)
		} else {
			log.Infof("Instance %s terminated", e.InstanceID)
		}
	}
}

// getAL2023AMI returns the latest Amazon Linux 2023 x86_64 AMI ID.
// AL2023 supports bash user-data via cloud-init; RHCOS uses Ignition and ignores bash scripts.
func (e *ExternalTargetInfra) getAL2023AMI(ctx context.Context) (string, error) {
	out, err := e.client.DescribeImages(ctx, &ec2.DescribeImagesInput{
		Owners: []string{"amazon"},
		Filters: []ec2types.Filter{
			{Name: aws.String("name"), Values: []string{"al2023-ami-2023*-x86_64"}},
			{Name: aws.String("state"), Values: []string{"available"}},
		},
	})
	if err != nil || len(out.Images) == 0 {
		return "", fmt.Errorf("no AL2023 AMI found: %w", err)
	}
	// Sort by creation date descending, return latest
	latest := out.Images[0]
	for _, img := range out.Images[1:] {
		if aws.ToString(img.CreationDate) > aws.ToString(latest.CreationDate) {
			latest = img
		}
	}
	log.Infof("Using Amazon Linux 2023 AMI: %s (%s)", aws.ToString(latest.ImageId), aws.ToString(latest.Name))
	return aws.ToString(latest.ImageId), nil
}

// findFreeIPs finds 'count' consecutive unused IPs in the given subnet
func (e *ExternalTargetInfra) findFreeIPs(ctx context.Context, subnetID string, count int) (string, string, error) {
	subnetOut, err := e.client.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		SubnetIds: []string{subnetID},
	})
	if err != nil || len(subnetOut.Subnets) == 0 {
		return "", "", fmt.Errorf("describe subnet %s: %w", subnetID, err)
	}
	cidr := aws.ToString(subnetOut.Subnets[0].CidrBlock)

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

	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", "", fmt.Errorf("parse CIDR %s: %w", cidr, err)
	}

	// AWS reserves first 4 addresses and last address
	hosts := []string{}
	ipNet.IP = ipNet.IP.Mask(ipNet.Mask)
	startInt := new(big.Int).SetBytes(ipNet.IP.To4())
	ones, bits := ipNet.Mask.Size()
	hostCount := (1 << (bits - ones)) - 2

	for i := 1; i <= hostCount; i++ {
		curr := new(big.Int).Add(startInt, big.NewInt(int64(i)))
		b := curr.Bytes()
		if len(b) < 4 {
			b = append(make([]byte, 4-len(b)), b...)
		}
		ipStr := fmt.Sprintf("%d.%d.%d.%d", b[0], b[1], b[2], b[3])
		// Skip AWS reserved (.1, .2, .3)
		if i <= 3 {
			continue
		}
		hosts = append(hosts, ipStr)
	}

	var freeIPs []string
	for _, ipStr := range hosts {
		if !used[ipStr] {
			freeIPs = append(freeIPs, ipStr)
			if len(freeIPs) == count {
				break
			}
		} else {
			freeIPs = freeIPs[:0]
		}
	}

	if len(freeIPs) < count {
		return "", "", fmt.Errorf("not enough consecutive free IPs in subnet %s (need %d, found %d)", subnetID, count, len(freeIPs))
	}

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

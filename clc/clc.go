package clc

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	clcsdk "github.com/CenturyLinkCloud/clc-sdk"
	"github.com/CenturyLinkCloud/clc-sdk/api"
	"github.com/CenturyLinkCloud/clc-sdk/lb"
	"github.com/CenturyLinkCloud/clc-sdk/server"
	"github.com/gliderlabs/registrator/bridge"
)

func init() {
	f := new(Factory)
	bridge.Register(f, "clc-lb")
}

type Factory struct{}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {
	ifaces, _ := net.Interfaces()
	for _, i := range ifaces {
		log.Printf("Interface name: %s\n", i.Name)
		addrss, _ := i.Addrs()
		for _, a := range addrss {
			log.Printf("Address network: %s", a.Network())
			log.Printf("Address : %s", a.String())
		}
	}

	// Are we running in debug mode CLC_REG_DEBUG
	debug := false
	if v := os.Getenv("CLC_REG_DEBUG"); v == "true" {
		debug = true
	}

	config, _ := api.EnvConfig()
	config.UserAgent = "Registrator/Clc-Provider"

	client := clcsdk.New(config)

	datacenter := uri.Host
	if datacenter == "" {
		datacenter = "GB3"
	}
	log.Println(uri.Host)

	return &ClcAdapter{client: client, datacenter: datacenter, debug: debug}
}

type ClcAdapter struct {
	client     *clcsdk.Client
	datacenter string
	debug      bool
}

// Ping will try to connect to clc by attempting to retrieve the list of data centeres.
func (r *ClcAdapter) Ping() error {
	r.debugMessage("Entering Ping")

	_, err := r.client.DC.Get(r.datacenter)
	if err != nil {
		return err
	}
	r.logMessage("Ping succesfull. Data center details retrieved for %s", r.datacenter)

	return nil
}

func (r *ClcAdapter) Register(service *bridge.Service) error {
	r.debugMessage("Enter Register")
	r.dumpService(service)
	r.dumpClcEnvironment()

	// get the clc attribute. If it doesn't exist or is set to alse then exist
	clcAttr := service.Attrs["clc"]
	if clcAttr != "true" {
		r.logMessage("Service %s not marked for CLC LB", service.Name)
		return nil
	}

	// Check that the port is 80 or 443
	if service.Origin.ExposedPort != "80" && service.Origin.ExposedPort != "443" {
		return errors.New("A CLC load balancer can only be creaed for port 80 or 443")
	}

	//TODO: Read the DC from Tags
	lbDetails, err := r.findOrCreateLoadBalancer(r.datacenter, service.Name)
	if err != nil {
		return err
	}

	portNumber, err := strconv.Atoi(service.Origin.ExposedPort)
	if err != nil {
		return err
	}

	pool, err := r.findOrCreatePool(r.datacenter, *lbDetails, portNumber)
	if err != nil {
		return err
	}

	// Get the internal IP address for the host
	internalIPAddress, err := r.findClcInternalIPByPublicIP(service.Origin.HostIP)
	if err != nil {
		return err
	}
	if internalIPAddress == "" {
		errMsg := fmt.Sprintf("The internal IP address for docker host %s couldn't be found", service.Origin.HostIP)
		return errors.New(errMsg)
	}
	r.logMessage("Found internal IP %s for docker host with public IP %s", internalIPAddress, service.Origin.HostIP)

	// Check the IP address / port combination isn't already in the pool
	nodeExists := false
	for _, node := range pool.Nodes {

		serviceHostPort, err := strconv.Atoi(service.Origin.HostPort)
		if err != nil {
			return err
		}

		if r.nodeMatchesService(node, internalIPAddress, serviceHostPort) {
			r.logMessage("A node with IP address %s and port %d already exists in load balancer %s", internalIPAddress, serviceHostPort, lbDetails.Name)
			nodeExists = true
		}
	}

	// Pool node doesn't exist so createdPool
	if nodeExists == false {
		newNode := new(lb.Node)
		newNode.IPaddress = internalIPAddress
		hostPort, err := strconv.Atoi(service.Origin.HostPort)
		if err != nil {
			return err
		}
		newNode.PrivatePort = hostPort

		poolNodes := r.addNode(pool.Nodes, *newNode)

		r.debugMessage("Register - calling SDK to update nodes in pool")
		err = r.client.LB.UpdateNodes(r.datacenter, lbDetails.ID, pool.ID, poolNodes...)
		r.debugMessage("Register - call to SDK to update nodes in pool complete")

		if err != nil {
			return err
		}
		r.logMessage("Added node with IP address %s and port %d to pool %s", internalIPAddress, hostPort, pool.ID)

	}

	return nil
}

func (r *ClcAdapter) Deregister(service *bridge.Service) error {
	r.debugMessage("Enter Deregister")

	// Get the load balancer
	lbDetails, err := r.findLoadBalancer(r.datacenter, service.Name)
	if err != nil {
		return err
	}

	// If there are any nodes within a pool then remove
	err = r.removeServiceFromLoadBalancer(service, *lbDetails)
	if err != nil {
		return err
	}

	//cleanup
	err = r.cleanupLoadbalancer(lbDetails.ID)
	if err != nil {
		return err
	}

	return nil
}

func (r *ClcAdapter) Refresh(service *bridge.Service) error {
	r.debugMessage("Enter Refresh")

	return nil
}

func (r *ClcAdapter) Services() ([]*bridge.Service, error) {
	r.debugMessage("Enter Services")

	return []*bridge.Service{}, nil
}

func (r *ClcAdapter) findOrCreatePool(dc string, loadBalancer lb.LoadBalancer, poolPort int) (*lb.Pool, error) {
	r.debugMessage("Enter findOrCreatePool")

	pool := r.findPool(dc, loadBalancer, poolPort)
	if pool != nil {
		return pool, nil
	}

	// Create a new pool as it wasn't found
	newPool := new(lb.Pool)
	newPool.Port = poolPort

	r.debugMessage("findOrCreatePool - calling sdk to create pool")
	createdPool, err := r.client.LB.CreatePool(dc, loadBalancer.ID, *newPool)
	r.debugMessage("findOrCreatePool - called sdk to create pool")

	if err != nil {
		return nil, err
	}
	r.logMessage("Created new pool for port number %s and load balancer %s. New pool id is %s", poolPort, loadBalancer.Name, createdPool.ID)

	return createdPool, nil
}

func (r *ClcAdapter) findPool(dc string, loadBalancer lb.LoadBalancer, poolPort int) *lb.Pool {
	r.debugMessage("Enter findPool")

	for _, pool := range loadBalancer.Pools {
		if pool.Port == poolPort {
			return &pool
		}
	}

	return nil
}

func (r *ClcAdapter) findOrCreateLoadBalancer(dc string, lbName string) (*lb.LoadBalancer, error) {
	r.debugMessage("Enter findOrCreateLoadBalancer")

	foundLb, err := r.findLoadBalancer(dc, lbName)
	if err != nil {
		return nil, err
	}

	if foundLb != nil {
		return foundLb, nil
	}

	// Load balancer wasn't found so create
	currentTime := time.Now()
	newLb := new(lb.LoadBalancer)
	newLb.Name = lbName
	newLb.Description = fmt.Sprintf("Created by registrator at %s", currentTime.Format("2006-01-02 15:02:02"))

	r.debugMessage("findOrCreateLoadBalancer - calling SDK to create LB")
	createdLb, err := r.client.LB.Create(dc, *newLb)
	r.debugMessage("findOrCreateLoadBalancer - called SDK to create LB")
	if err != nil {
		return nil, err
	}
	r.logMessage("Created new load balancer with name %s. New load balancer id is %s and IP is %s", lbName, createdLb.ID, createdLb.IPaddress)

	// Sleeping to allow time for backend to catchup up
	time.Sleep(1 * time.Second)

	return createdLb, nil
}

func (r *ClcAdapter) findLoadBalancer(dc string, lbName string) (*lb.LoadBalancer, error) {
	r.debugMessage("Enter findLoadBalancer")

	r.debugMessage("findLoadBalancer - calling SDK to get all LBs")
	foundLb, err := r.client.LB.GetAll(r.datacenter)
	r.debugMessage("findLoadBalancer - called SDK to get all LBs")

	if err != nil {
		return nil, err
	}

	// Loop round and find first with a matching name
	for _, loadBalancer := range foundLb {
		if loadBalancer.Name == lbName {
			return loadBalancer, nil
		}
	}

	return nil, nil
}

func (r *ClcAdapter) addNode(nodes []lb.Node, node lb.Node) []lb.Node {
	r.debugMessage("Enter addNode")

	currentLen := len(nodes)
	if currentLen == cap(nodes) {
		newPoolNodes := make([]lb.Node, currentLen, currentLen+1)
		copy(newPoolNodes, nodes)
		nodes = newPoolNodes
	}
	nodes = nodes[0 : currentLen+1]
	nodes[currentLen] = node

	return nodes
}

func (r *ClcAdapter) removeServiceFromLoadBalancer(service *bridge.Service, loadBalancer lb.LoadBalancer) error {
	r.debugMessage("Enter removeServiceFromLoadBalancer")

	// Remove service from pools
	for _, pool := range loadBalancer.Pools {
		err := r.removeServiceFromPool(service, loadBalancer, pool)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ClcAdapter) removeServiceFromPool(service *bridge.Service, loadBalanacer lb.LoadBalancer, pool lb.Pool) error {
	r.debugMessage("Enter removeServiceFromPool")

	// Get the internal IP address for the host
	internalIPAddress, err := r.findClcInternalIPByPublicIP(service.Origin.HostIP)
	if err != nil {
		return err
	}
	r.debugMessage("removeServiceFromPool - found internal IP address: %s\n", internalIPAddress)

	for i, node := range pool.Nodes {
		serviceHostPort, err := strconv.Atoi(service.Origin.HostPort)
		if err != nil {
			return err
		}

		if r.nodeMatchesService(node, internalIPAddress, serviceHostPort) {
			r.debugMessage("removeServiceFromPool - found matching node\n")

			//NOTE: assumption is that there is one match only. This needs testing
			pool.Nodes = append(pool.Nodes[:i], pool.Nodes[i+1:]...)

			r.debugMessage("removeServiceFromPool - calling SDK to update nodes in LB pool")
			err := r.client.LB.UpdateNodes(r.datacenter, loadBalanacer.ID, pool.ID, pool.Nodes...)
			r.debugMessage("removeServiceFromPool - called SDK to update nodes in LB pool")

			if err != nil {
				return err
			}

			r.logMessage("Updated nodes in pool %s", pool.ID)
			break
		}
	}
	return nil
}

func (r *ClcAdapter) nodeMatchesService(node lb.Node, serviceHostInternalIPAddress string, serviceHostPort int) bool {
	r.debugMessage("Enter nodeMatchesService")

	return node.IPaddress == serviceHostInternalIPAddress && node.PrivatePort == serviceHostPort
}

func (r *ClcAdapter) cleanupLoadbalancer(loadBalancerID string) error {
	r.debugMessage("Enter cleanupLoadbalancer")

	r.debugMessage("cleanupLoadbalancer - calling SDK to get load balancer details")
	loadBalancer, err := r.client.LB.Get(r.datacenter, loadBalancerID)
	r.debugMessage("cleanupLoadbalancer - called SDK to get load balancer detailsl")

	if err != nil {
		return err
	}

	// If all the pools are empty then delete the loadBalancer
	if r.poolsAreEmpty(loadBalancer.Pools) {

		r.debugMessage("cleanupLoadbalancer - calling SDK delete load balancer")
		err := r.client.LB.Delete(r.datacenter, loadBalancer.ID)
		r.debugMessage("cleanupLoadbalancer - called SDK delete load balancer")

		if err != nil {
			return err
		}

		r.logMessage("Deleted load balanacer %s as all pools are empty", loadBalancer.Name)
		return nil
	}

	// Delete empty pools
	err = r.deleteEmptyPools(loadBalancer.Pools, loadBalancer.ID)
	if err != nil {
		return err
	}

	return nil
}

func (r *ClcAdapter) poolsAreEmpty(pools []lb.Pool) bool {
	r.debugMessage("Enter  poolsAreEmpty")

	for _, pool := range pools {
		if len(pool.Nodes) > 0 {
			return false
		}
	}
	return true
}

func (r *ClcAdapter) deleteEmptyPools(pools []lb.Pool, loadBalancerID string) error {
	r.debugMessage("Enter deleteEmptyPools")

	for _, pool := range pools {
		if len(pool.Nodes) == 0 {

			r.debugMessage("deleteEmptyPools - calling sdk to delete pool")
			err := r.client.LB.DeletePool(r.datacenter, loadBalancerID, pool.ID)
			r.debugMessage("deleteEmptyPools - called sdk to delete pool")

			if err != nil {
				return err
			}

			r.logMessage("Deleted pool %s as it contained no nodes", pool.ID)
		}
	}

	return nil
}

func (r *ClcAdapter) findClcInternalIPByPublicIP(publicIP string) (string, error) {
	r.debugMessage("Enter findClcInternalIPByPublicIP")

	r.debugMessage("findClcInternalIPByPublicIP - calling sdk to get DC details")
	resp, err := r.client.DC.Get(r.datacenter)
	r.debugMessage("findClcInternalIPByPublicIP - called sdk to get DC details")

	if err != nil {
		return "", err
	}

	for _, link := range resp.Links {
		if link.Rel == "group" {
			internalIP, err := r.findServerInternalIPInGroupByPublicIP(publicIP, link.ID)
			if err != nil {
				return "", err
			}
			if internalIP != "" {
				return internalIP, nil
			}
		}
	}

	return "", nil
}

func (r *ClcAdapter) findServerInternalIPInGroupByPublicIP(publicIP string, groupID string) (string, error) {
	r.debugMessage("Enter findServerInternalIPInGroupByPublicIP")

	r.debugMessage("findServerInternalIPInGroupByPublicIP - calling sdk to get group details")
	resp, err := r.client.Group.Get(groupID)
	r.debugMessage("findServerInternalIPInGroupByPublicIP - called sdk to get group details")

	if err != nil {
		return "", err
	}

	// Check the current groups servers first
	for _, serverName := range resp.Servers() {
		r.debugMessage("findServerInternalIPInGroupByPublicIP - calling sdk to get server details")
		serverResp, err := r.client.Server.Get(serverName)
		r.debugMessage("findServerInternalIPInGroupByPublicIP - called sdk to get server details")

		if err != nil {
			return "", err
		}
		internalIP := r.serverGetInteralFromPublic(*serverResp, publicIP)
		if internalIP != "" {
			return internalIP, nil
		}
	}

	// Loop round the subgroups
	for _, subGroup := range resp.Groups {
		serverNameResp, err := r.findServerInternalIPInGroupByPublicIP(publicIP, subGroup.ID)
		if err != nil {
			return "", err
		}
		if serverNameResp != "" {
			return serverNameResp, nil
		}
	}

	// Nothing found so return
	return "", nil
}

func (r *ClcAdapter) serverGetInteralFromPublic(server server.Response, ipAddress string) string {
	r.debugMessage("Enter serverGetInteralFromPublic")

	if len(server.Details.IPaddresses) == 0 {
		return ""
	}

	for _, address := range server.Details.IPaddresses {
		if address.Public == ipAddress {
			return address.Internal
		}
	}

	return ""
}

func (r *ClcAdapter) logMessage(message string, v ...interface{}) {
	formattedMessage := fmt.Sprintf(message, v)
	log.Printf("CLC - INFO - %s\n", formattedMessage)
}

func (r *ClcAdapter) debugMessage(message string, v ...interface{}) {
	if r.debug {
		formattedMessage := fmt.Sprintf(message, v)
		log.Printf("CLC - DBG - %s\n", formattedMessage)
	}
}

func (r *ClcAdapter) dumpService(service *bridge.Service) {
	r.debugMessage("Service Name: %s\n", service.Name)
	r.debugMessage("Service ID: %s\n", service.ID)
	r.debugMessage("Service IP: %s\n", service.IP)
	r.debugMessage("Service Port: %s\n", string(service.Port))
	r.debugMessage("Service TTL: %s\n", string(service.TTL))
	r.debugMessage("Service Tags: %s\n", service.Tags)
	r.debugMessage("Service Attrs: %s\n", service.Attrs)
	for key, value := range service.Attrs {
		r.debugMessage("Attribute %s = %s\n", key, value)
	}
	r.debugMessage("Origin Container Hostname: %s\n", service.Origin.ContainerHostname)
	r.debugMessage("Origin Container ID: %s\n", service.Origin.ContainerID)
	r.debugMessage("Origin Container Name: %s\n", service.Origin.ContainerName)
	r.debugMessage("Origin Exposed IP: %s\n", service.Origin.ExposedIP)
	r.debugMessage("Origin Exposed Port: %s\n", service.Origin.ExposedPort)
	r.debugMessage("Origin Host IP: %s\n", service.Origin.HostIP)
	r.debugMessage("Origin Host Port: %s\n", service.Origin.HostPort)
	r.debugMessage("Origin Port Type: %s\n", service.Origin.PortType)
}

func (r *ClcAdapter) dumpClcEnvironment() {
	r.debugMessage("CLC_REG_DEBUG: %s\n", os.Getenv("CLC_REG_DEBUG"))
	r.debugMessage("CLC_USERNAME: %s\n", os.Getenv("CLC_USERNAME"))
	r.debugMessage("CLC_USER: %s\n", os.Getenv("CLC_USER"))
	r.debugMessage("CLC_PASSWORD: %s\n", os.Getenv("CLC_PASSWORD"))
	r.debugMessage("CLC_ALIAS: %s\n", os.Getenv("CLC_ALIAS"))
}

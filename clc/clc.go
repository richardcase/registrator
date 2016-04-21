package clc

import (
	"log"
	"net/url"
	"strconv"

	clcsdk "github.com/CenturyLinkCloud/clc-sdk"
	"github.com/CenturyLinkCloud/clc-sdk/api"
	"github.com/CenturyLinkCloud/clc-sdk/lb"
	"github.com/gliderlabs/registrator/bridge"
)

func init() {
	f := new(Factory)
	bridge.Register(f, "clc-lb")
}

//func (r *ClcAdapter) interpolateService(script string, service *bridge.Service) string {
//	withIp := strings.Replace(script, "$SERVICE_IP", service.Origin.HostIP, -1)
//	withPort := strings.Replace(withIp, "$SERVICE_PORT", service.Origin.HostPort, -1)
//	return withPort
//}

type Factory struct{}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {
	log.Println("In clc New")
	config, _ := api.EnvConfig()
	config.UserAgent = "Registrator/Clc-Provider"

	client := clcsdk.New(config)
	//TODO: read the datacenter from the URL
	return &ClcAdapter{client: client, datacenter: "GB3"}
}

type ClcAdapter struct {
	client     *clcsdk.Client
	datacenter string
}

// Ping will try to connect to clc by attempting to retrieve the list of data centeres.
func (r *ClcAdapter) Ping() error {
	log.Println("In clc Ping")
	dcResp, err := r.client.DC.GetAll()
	if err != nil {
		return err
	}
	log.Println("clc: number of data centers accessible ", len(dcResp))

	return nil
}

func (r *ClcAdapter) Register(service *bridge.Service) error {
	log.Println("In clc Register")
	dumpService(service)
	log.Printf("CLC = %s\n", service.Attrs["clc"])

	// get the clc attribute. If it doesn't exist or is set to alse then exist
	clcAttr := service.Attrs["clc"]
	if clcAttr != "true" {
		log.Printf("Service %s not marked for CLC LB", service.Name)
		return nil
	}

	//TODO: Read the DC from Tags
	lbDetails, err := r.findOrCreateLoadBalancer(r.datacenter, service.Name)
	if err != nil {
		return err
	}

	pool, err := r.findOrCreatePool(r.datacenter, *lbDetails, service.Port)
	if err != nil {
		return err
	}

	// Check the IP address / port combination isn't already in the pool
	nodeExists := false
	for _, node := range pool.Nodes {
		if node.IPaddress == service.Origin.ExposedIP &&
			string(node.PrivatePort) == service.Origin.ExposedPort {
			nodeExists = true
		}
	}

	// Pool node doesn't exist so createdPool
	if nodeExists == false {
		newNode := new(lb.Node)
		newNode.IPaddress = service.Origin.ExposedIP
		exposedPort, err := strconv.Atoi(service.Origin.ExposedPort)
		if err != nil {
			return err
		}
		newNode.PrivatePort = exposedPort

		pool.Nodes = r.addNodeToPool(pool.Nodes, *newNode)

		err = r.client.LB.UpdatePool(r.datacenter, lbDetails.ID, pool.ID, *pool)
		if err != nil {
			return err
		}
	}

	return nil

}

func (r *ClcAdapter) Deregister(service *bridge.Service) error {
	log.Println("In clc Deregister")
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

	return nil
}

func (r *ClcAdapter) Refresh(service *bridge.Service) error {
	log.Println("In clc Refresh")
	return nil
}

func (r *ClcAdapter) Services() ([]*bridge.Service, error) {
	log.Println("In clc Services")
	return []*bridge.Service{}, nil
}

func (r *ClcAdapter) findOrCreatePool(dc string, loadBalancer lb.LoadBalancer, poolPort int) (*lb.Pool, error) {
	pool := r.findPool(dc, loadBalancer, poolPort)
	if pool != nil {
		return pool, nil
	}

	// Create a new pool as it wasn't found
	newPool := new(lb.Pool)
	newPool.Port = poolPort

	createdPool, err := r.client.LB.CreatePool(dc, loadBalancer.ID, *newPool)
	if err != nil {
		return nil, err
	}

	return createdPool, nil
}

func (r *ClcAdapter) findPool(dc string, loadBalancer lb.LoadBalancer, poolPort int) *lb.Pool {
	for _, pool := range loadBalancer.Pools {
		if pool.Port == poolPort {
			return &pool
		}
	}
	return nil
}

func (r *ClcAdapter) findOrCreateLoadBalancer(dc string, lbName string) (*lb.LoadBalancer, error) {
	log.Println("In clc findOrCreateLoadBalancer")
	foundLb, err := r.findLoadBalancer(dc, lbName)
	if err != nil {
		return nil, err
	}

	if foundLb != nil {
		return foundLb, nil
	}

	// Load balancer wasn't found so create
	log.Println("In clc: load balancer not found so about to create")
	newLb := new(lb.LoadBalancer)
	newLb.Name = lbName
	newLb.Description = "Created by registrator" //TODO: add extra detail

	log.Println("In clc: calling LB create SDK")
	createdLb, err := r.client.LB.Create(dc, *newLb)
	if err != nil {
		return nil, err
	}

	return createdLb, nil
}

func (r *ClcAdapter) findLoadBalancer(dc string, lbName string) (*lb.LoadBalancer, error) {
	log.Println("In clc findLoadBalancer")
	foundLb, err := r.client.LB.GetAll(r.datacenter)

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

func (r *ClcAdapter) addNodeToPool(nodes []lb.Node, node lb.Node) []lb.Node {
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
	for i, node := range pool.Nodes {
		if r.nodeMatchesService(node, service) {
			//NOTE: assumption is that there is one match only. This needs testing
			pool.Nodes = append(pool.Nodes[:i], pool.Nodes[i+1:]...)

			err := r.client.LB.UpdatePool(r.datacenter, loadBalanacer.ID, pool.ID, pool)
			if err != nil {
				return err
			}
			break
		}
	}
	return nil
}

func (r *ClcAdapter) nodeMatchesService(node lb.Node, service *bridge.Service) bool {
	return node.IPaddress == service.Origin.ExposedIP && string(node.PrivatePort) == service.Origin.ExposedPort
}

func (r *ClcAdapter) cleanupLoadbalancer(loadBalancerID string) error {
	loadBalancer, err := r.client.LB.Get(r.datacenter, loadBalancerID)
	if err != nil {
		return err
	}

	// If all the pools are empty then delete the loadBalancer
	if r.poolsAreEmpty(loadBalancer.Pools) {
		err := r.client.LB.Delete(r.datacenter, loadBalancer.ID)
		if err != nil {
			return err
		}
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
	for _, pool := range pools {
		if len(pool.Nodes) > 0 {
			return false
		}
	}
	return true
}

func (r *ClcAdapter) deleteEmptyPools(pools []lb.Pool, loadBalancerID string) error {
	for _, pool := range pools {
		if len(pool.Nodes) == 0 {
			err := r.client.LB.DeletePool(r.datacenter, loadBalancerID, pool.ID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func dumpService(service *bridge.Service) {
	log.Println("In clc Register")
	log.Printf("Service Name: %s\n", service.Name)
	log.Printf("Service ID: %s\n", service.ID)
	log.Printf("Service IP: %s\n", service.IP)
	log.Printf("Service Port: %s\n", string(service.Port))
	log.Printf("Service TTL: %s\n", string(service.TTL))
	log.Printf("Service Tags: %s\n", service.Tags)
	log.Printf("Service Attrs: %s\n", service.Attrs)
	for key, value := range service.Attrs {
		log.Printf("Attribute %s = %s\n", key, value)
	}
}

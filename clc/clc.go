package clc

import (
	"fmt"
	"log"
	"net/url"
	//"strconv"
	"strings"

	clcapi "github.com/CenturyLinkCloud/clc-sdk"
	"github.com/CenturyLinkCloud/clc-sdk/api"
	"github.com/gliderlabs/registrator/bridge"
)

const DefaultInterval = "10s"

func init() {
	f := new(Factory)
	bridge.Register(f, "clc-lb")
}

func (r *ClcAdapter) interpolateService(script string, service *bridge.Service) string {
	withIp := strings.Replace(script, "$SERVICE_IP", service.Origin.HostIP, -1)
	withPort := strings.Replace(withIp, "$SERVICE_PORT", service.Origin.HostPort, -1)
	return withPort
}

type Factory struct{}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {
	config, _ := api.EnvConfig()
	config.UserAgent = "Registrator/Clc-Provider"

	client := clcapi.New(config)
	return &ClcAdapter{client: client}
}

type ClcAdapter struct {
	client *clcapi.Client
}

// Ping will try to connect to consul by attempting to retrieve the current leader.
func (r *ClcAdapter) Ping() error {
	status := r.client.Status()
	leader, err := status.Leader()
	if err != nil {
		return err
	}
	log.Println("consul: current leader ", leader)

	return nil
}

func (r *ClcAdapter) Register(service *bridge.Service) error {
	registration := new(consulapi.AgentServiceRegistration)
	registration.ID = service.ID
	registration.Name = service.Name
	registration.Port = service.Port
	registration.Tags = service.Tags
	registration.Address = service.IP
	registration.Check = r.buildCheck(service)
	return r.client.Agent().ServiceRegister(registration)
}

func (r *ClcAdapter) buildCheck(service *bridge.Service) *consulapi.AgentServiceCheck {
	check := new(consulapi.AgentServiceCheck)
	if path := service.Attrs["check_http"]; path != "" {
		check.HTTP = fmt.Sprintf("http://%s:%d%s", service.IP, service.Port, path)
		if timeout := service.Attrs["check_timeout"]; timeout != "" {
			check.Timeout = timeout
		}
	} else if cmd := service.Attrs["check_cmd"]; cmd != "" {
		check.Script = fmt.Sprintf("check-cmd %s %s %s", service.Origin.ContainerID[:12], service.Origin.ExposedPort, cmd)
	} else if script := service.Attrs["check_script"]; script != "" {
		check.Script = r.interpolateService(script, service)
	} else if ttl := service.Attrs["check_ttl"]; ttl != "" {
		check.TTL = ttl
	} else if tcp := service.Attrs["check_tcp"]; tcp != "" {
		check.TCP = fmt.Sprintf("%s:%d", service.IP, service.Port)
		if timeout := service.Attrs["check_timeout"]; timeout != "" {
			check.Timeout = timeout
		}
	} else {
		return nil
	}
	if check.Script != "" || check.HTTP != "" || check.TCP != "" {
		if interval := service.Attrs["check_interval"]; interval != "" {
			check.Interval = interval
		} else {
			check.Interval = DefaultInterval
		}
	}
	return check
}

func (r *ClcAdapter) Deregister(service *bridge.Service) error {
	return r.client.Agent().ServiceDeregister(service.ID)
}

func (r *ClcAdapter) Refresh(service *bridge.Service) error {
	return nil
}

func (r *ClcAdapter) Services() ([]*bridge.Service, error) {
	services, err := r.client.Agent().Services()
	if err != nil {
		return []*bridge.Service{}, err
	}
	out := make([]*bridge.Service, len(services))
	i := 0
	for _, v := range services {
		s := &bridge.Service{
			ID:   v.ID,
			Name: v.Service,
			Port: v.Port,
			Tags: v.Tags,
			IP:   v.Address,
		}
		out[i] = s
		i++
	}
	return out, nil
}

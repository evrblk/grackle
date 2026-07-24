package commands

import (
	"fmt"
	"strings"

	"github.com/evrblk/monstera"
	"github.com/spf13/cobra"
)

// monsteraNodesFlags holds the mutually-exclusive node-discovery flags shared by
// the client-side commands (gateway, worker). Discovery answers "which nodes do I
// ask for the cluster config"; a polling config provider then learns the config
// from those nodes and refreshes as the topology changes.
type monsteraNodesFlags struct {
	static string // comma-separated host:port list
	file   string // path to a file of host:port lines
	srv    string // DNS SRV record name
}

func addMonsteraNodesFlags(cmd *cobra.Command, f *monsteraNodesFlags) {
	cmd.PersistentFlags().StringVar(&f.static, "monstera-nodes", "", "Comma-separated node gRPC addresses to discover the cluster config from")
	cmd.PersistentFlags().StringVar(&f.file, "monstera-nodes-file", "", "Path to a file of node gRPC addresses (one host:port per line)")
	cmd.PersistentFlags().StringVar(&f.srv, "monstera-nodes-srv", "", "DNS SRV record name to resolve node gRPC addresses from")
}

// buildNodeDiscovery builds a NodeDiscovery from the flags. Exactly one of the
// three must be set.
func buildNodeDiscovery(f monsteraNodesFlags) (monstera.NodeDiscovery, error) {
	set := 0
	for _, s := range []string{f.static, f.file, f.srv} {
		if s != "" {
			set++
		}
	}
	if set != 1 {
		return nil, fmt.Errorf("exactly one of --monstera-nodes, --monstera-nodes-file, --monstera-nodes-srv must be set")
	}

	switch {
	case f.static != "":
		var addrs []string
		for _, a := range strings.Split(f.static, ",") {
			if a = strings.TrimSpace(a); a != "" {
				addrs = append(addrs, a)
			}
		}
		return monstera.NewStaticNodeDiscovery(addrs), nil
	case f.file != "":
		return monstera.NewFileNodeDiscovery(f.file), nil
	default:
		return monstera.NewSRVNodeDiscovery(f.srv, nil), nil
	}
}

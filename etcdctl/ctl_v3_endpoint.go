// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3"
)

var (
	endpointMemberPrometheus bool
	endpointHealthCluster    bool
)

// NewEndpointCommand returns the cobra command for "endpoint".
func NewEndpointCommand() *cobra.Command {
	ec := &cobra.Command{
		Use:   "endpoint <subcommand>",
		Short: "Endpoint related commands",
	}

	ec.AddCommand(newEndpointHealthCommand())
	ec.AddCommand(newEndpointStatusCommand())
	ec.AddCommand(newEndpointHashKVCommand())

	return ec
}

func newEndpointHealthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Checks the healthiness of endpoints specified in `--endpoints` flag",
		Run:   endpointHealthCommandFunc,
	}
	cmd.Flags().BoolVar(&endpointHealthCluster, "cluster", false, "use all endpoints from the member list of the cluster")
	return cmd
}

type epHealth struct {
	ep   string
	err  error
	took time.Duration
}

func endpointHealthCommandFunc(cmd *cobra.Command, args []string) { 
	endpoints, err := cmd.Flags().GetStringSlice("endpoints")
	if err != nil {
		ExitWithError(ExitError, err)
	}

	if endpointHealthCluster {
		endpoints = nil
		ctx, cancel := commandCtx(cmd)
		c := mustClient(cmd)
		resp, err := c.MemberList(ctx)
		cancel()
		if err != nil {
			ExitWithError(ExitError, err)
		}
		for _, memb := range resp.Members {
			endpoints = append(endpoints, memb.ClientURLs...)
		}
	}

	cfgs := make([]*clientv3.Config, len(endpoints))
	for i, ep := range endpoints {
		cfg := *mustClientConfig(cmd)
		cfg.Endpoints = []string{ep}
		cfgs[i] = &cfg
	}

	var wg sync.WaitGroup
	hch := make(chan epHealth, len(endpoints))
	for i := range endpoints {
		wg.Add(1)
		go func(cfg *clientv3.Config) {
			defer wg.Done()
			ep := cfg.Endpoints[0]
			cli, err := clientv3.New(*cfg)
			if err != nil {
				hch <- epHealth{ep: ep, err: err}
				return
			}
			defer cli.Close()

			st := time.Now()
			// check connection health
			ctx, cancel := commandCtx(cmd)
			_, err = cli.Get(ctx, "a")
			cancel()
			if err != nil && err != clientv3.ErrNoLeader {
				hch <- epHealth{ep: ep, err: err}
				return
			}
			hch <- epHealth{ep: ep, took: time.Since(st)}
		}(cfgs[i])
	}

	wg.Wait()
	close(hch)

	errNum := 0
	for h := range hch {
		if h.err != nil {
			fmt.Fprintf(os.Stderr, "%s is unhealthy: failed to commit proposal: %v\n", h.ep, h.err)
			errNum++
		} else {
			fmt.Printf("%s is healthy: successfully committed proposal: took = %v\n", h.ep, h.took)
		}
	}

	if errNum > 0 {
		ExitWithError(ExitError, fmt.Errorf("unhealthy cluster"))
	}
}

func newEndpointStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Prints out the status of endpoints specified in `--endpoints` flag",
		Run:   endpointStatusCommandFunc,
	}
	return cmd
}

func endpointStatusCommandFunc(cmd *cobra.Command, args []string) {
	endpoints, err := cmd.Flags().GetStringSlice("endpoints")
	if err != nil {
		ExitWithError(ExitError, err)
	}

	cfgs := make([]*clientv3.Config, len(endpoints))
	for i, ep := range endpoints {
		cfg := *mustClientConfig(cmd)
		cfg.Endpoints = []string{ep}
		cfgs[i] = &cfg
	}

	var wg sync.WaitGroup
	type epStatus struct {
		ep   string
		resp *clientv3.StatusResponse
		err  error
	}
	sch := make(chan epStatus, len(endpoints))

	for i := range endpoints {
		wg.Add(1)
		go func(cfg *clientv3.Config) {
			defer wg.Done()
			ep := cfg.Endpoints[0]
			cli, err := clientv3.New(*cfg)
			if err != nil {
				sch <- epStatus{ep: ep, err: err}
				return
			}
			defer cli.Close()

			ctx, cancel := commandCtx(cmd)
			resp, err := cli.Status(ctx, ep)
			cancel()
			sch <- epStatus{ep: ep, resp: resp, err: err}
		}(cfgs[i])
	}

	wg.Wait()
	close(sch)

	rcs := make([]epStatus, 0, len(endpoints))
	for i := 0; i < len(endpoints); i++ {
		rcs = append(rcs, <-sch)
	}

	if displayType == displaySimple {
		for _, r := range rcs {
			if r.err != nil {
				fmt.Fprintf(os.Stderr, "Failed to get the status of endpoint %s (%v)\n", r.ep, r.err)
				continue
			}
			fmt.Printf("%s, %x, %s, %s, %t, %t, %d, %d, %d, \n", r.ep, r.resp.Header.MemberId, r.resp.Version, formatBytes(r.resp.DbSize), r.resp.IsLeader, r.resp.IsLearner, r.resp.RaftTerm, r.resp.RaftIndex, r.resp.Header.RaftTerm)
		}
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ENDPOINT", "ID", "VERSION", "DB SIZE", "IS LEADER", "IS LEARNER", "RAFT TERM", "RAFT INDEX", "RAFT APPLIED INDEX", "ERRORS"})
	for _, r := range rcs {
		if r.err != nil {
			table.Append([]string{r.ep, "", "", "", "", "", "", "", "", r.err.Error()})
			continue
		}
		table.Append([]string{
			r.ep,
			fmt.Sprintf("%x", r.resp.Header.MemberId),
			r.resp.Version,
			fmt.Sprintf("%s", formatBytes(r.resp.DbSize)),
			fmt.Sprintf("%t", r.resp.IsLeader),
			fmt.Sprintf("%t", r.resp.IsLearner),
			fmt.Sprintf("%d", r.resp.RaftTerm),
			fmt.Sprintf("%d", r.resp.RaftIndex),
			fmt.Sprintf("%d", r.resp.Header.RaftTerm),
			"",
		})
	}
	table.Render()
}

func newEndpointHashKVCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hashkv",
		Short: "Prints the KV history hash for each endpoint in `--endpoints` flag",
		Run:   endpointHashKVCommandFunc,
	}
	return cmd
}

func endpointHashKVCommandFunc(cmd *cobra.Command, args []string) { 
	endpoints, err := cmd.Flags().GetStringSlice("endpoints")
	if err != nil {
		ExitWithError(ExitError, err)
	}

	cfgs := make([]*clientv3.Config, len(endpoints))
	for i, ep := range endpoints {
		cfg := *mustClientConfig(cmd)
		cfg.Endpoints = []string{ep}
		cfgs[i] = &cfg
	}

	var wg sync.WaitGroup
	type epHash struct {
		ep   string
		resp *clientv3.HashKVResponse
		err  error
	}
	hch := make(chan epHash, len(endpoints))

	for i := range endpoints {
		wg.Add(1)
		go func(cfg *clientv3.Config) {
			defer wg.Done()
			ep := cfg.Endpoints[0]
			cli, err := clientv3.New(*cfg)
			if err != nil {
				hch <- epHash{ep: ep, err: err}
				return
			}
			defer cli.Close()

			ctx, cancel := commandCtx(cmd)
			resp, err := cli.HashKV(ctx, ep, 0)
			cancel()
			hch <- epHash{ep: ep, resp: resp, err: err}
		}(cfgs[i])
	}

	wg.Wait()
	close(hch)

	rcs := make([]epHash, 0, len(endpoints))
	for i := 0; i < len(endpoints); i++ {
		rcs = append(rcs, <-hch)
	}

	if displayType == displaySimple {
		for _, r := range rcs {
			if r.err != nil {
				fmt.Fprintf(os.Stderr, "Failed to get the hash of endpoint %s (%v)\n", r.ep, r.err)
				continue
			}
			fmt.Printf("%s, %x, %d\n", r.ep, r.resp.Header.MemberId, r.resp.Hash)
		}
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ENDPOINT", "ID", "HASH", "ERRORS"})
	for _, r := range rcs {
		if r.err != nil {
			table.Append([]string{r.ep, "", "", r.err.Error()})
			continue
		}
		table.Append([]string{
			r.ep,
			fmt.Sprintf("%x", r.resp.Header.MemberId),
			fmt.Sprintf("%d", r.resp.Hash),
			"",
		})
	}
	table.Render()
}
// Command etcdnode runs one embedded etcd member as its own OS process.
package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

func main() {
	name := flag.String("name", "", "member name")
	data := flag.String("data", "", "data directory")
	listenClient := flag.String("listen-client", "", "client listen URL")
	advertiseClient := flag.String("advertise-client", "", "client advertise URL")
	listenPeer := flag.String("listen-peer", "", "peer listen URL")
	advertisePeer := flag.String("advertise-peer", "", "peer advertise URL")
	initial := flag.String("initial-cluster", "", "name=url,...")
	token := flag.String("token", "relay", "initial cluster token")
	flag.Parse()
	if *name == "" || *data == "" || *listenClient == "" || *advertiseClient == "" || *listenPeer == "" || *advertisePeer == "" || *initial == "" {
		fmt.Fprintln(os.Stderr, "missing etcdnode flags")
		os.Exit(2)
	}
	cfg := embed.NewConfig()
	cfg.Name = *name
	cfg.Dir = *data
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.NewNop())
	cfg.ListenPeerUrls = mustURLs(*listenPeer)
	cfg.AdvertisePeerUrls = mustURLs(*advertisePeer)
	cfg.ListenClientUrls = mustURLs(*listenClient)
	cfg.AdvertiseClientUrls = mustURLs(*advertiseClient)
	cfg.InitialCluster = *initial
	cfg.InitialClusterToken = *token
	cfg.ClusterState = embed.ClusterStateFlagNew
	cfg.TickMs = 100
	cfg.ElectionMs = 500
	srv, err := embed.StartEtcd(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	select {
	case <-srv.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		srv.Close()
		fmt.Fprintln(os.Stderr, "ready timeout")
		os.Exit(1)
	}
	fmt.Printf("{\"event\":\"etcd_ready\",\"name\":%q,\"client\":%q}\n", *name, *advertiseClient)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	case <-srv.Server.StopNotify():
	}
	srv.Server.HardStop()
	srv.Close()
}

func mustURLs(raw string) []url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return []url.URL{*u}
}

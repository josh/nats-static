package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

const version = "0.0.1"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}

	configPath := os.Getenv("NATS_STATIC_CONFIG")
	if configPath == "" {
		configPath = "/etc/nats-static/config.json"
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	opts, err := cfg.natsOptions()
	if err != nil {
		log.Fatalf("nats auth: %v", err)
	}
	opts = append(opts,
		nats.Name("nats-static"),
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
	)

	nc, err := nats.Connect(cfg.NATS.URL, opts...)
	if err != nil {
		log.Fatalf("nats connect: %v", err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	// The bucket is provisioned externally (e.g. by NACK); wait for it to exist.
	var obs nats.ObjectStore
	for {
		obs, err = js.ObjectStore(cfg.ObjectStore.Bucket)
		if err == nil {
			break
		}
		log.Printf("object store %q not ready (%v); retrying", cfg.ObjectStore.Bucket, err)
		time.Sleep(2 * time.Second)
	}
	log.Printf("object store %q opened", cfg.ObjectStore.Bucket)

	srv := &server{obs: obs}
	mgr := newSessionManager(nc, obs, cfg.Multipart.MaxSessions, cfg.idleTimeout())

	subscribe := func(subject string, h nats.MsgHandler) {
		if _, err := nc.QueueSubscribe(subject, queueGroup, h); err != nil {
			log.Fatalf("subscribe %s: %v", subject, err)
		}
	}

	// Each write verb is reachable bare (unrestricted) or prefix-scoped via
	// "<verb>.<prefix>"; the server enforces that the target Path stays within
	// the prefix, so a NATS export can delegate a path namespace to an account.
	subscribe(putSubject, srv.put)
	subscribe(putSubject+".>", srv.put)
	// static.put.begin opens a multipart upload session for files larger than the
	// NATS max payload; the session itself runs on the replica that answers here.
	subscribe(beginSubject, mgr.begin)
	subscribe(beginSubject+".>", mgr.begin)
	// delete is a separate verb so it can be withheld from writers granted only put access.
	subscribe(deleteSubject, srv.del)
	subscribe(deleteSubject+".>", srv.del)

	log.Printf("serving HTTP on %s", cfg.HTTP.Listen)
	if err := http.ListenAndServe(cfg.HTTP.Listen, srv.mux()); err != nil {
		log.Fatalf("http: %v", err)
	}
}

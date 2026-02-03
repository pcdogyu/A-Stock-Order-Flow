package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/config"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/collector"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/memstore"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/runtimecfg"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/store/sqlite"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cmd := os.Args[1]
	switch cmd {
	case "init-db":
		fs := flag.NewFlagSet("init-db", flag.ExitOnError)
		cfgPath := fs.String("config", "configs/config.yaml", "config path (YAML)")
		_ = fs.Parse(os.Args[2:])

		cfg, err := config.Load(*cfgPath)
		fatalIf(err)
		db, err := sqlite.Open(cfg.DBPath)
		fatalIf(err)
		defer db.Close()
		fatalIf(sqlite.Migrate(db))
		log.Printf("db initialized: %s", cfg.DBPath)
	case "rt":
		fs := flag.NewFlagSet("rt", flag.ExitOnError)
		cfgPath := fs.String("config", "configs/config.yaml", "config path (YAML)")
		_ = fs.Parse(os.Args[2:])

		cfg, err := config.Load(*cfgPath)
		fatalIf(err)
		db, err := sqlite.Open(cfg.DBPath)
		fatalIf(err)
		defer db.Close()
		fatalIf(sqlite.Migrate(db))

		ctx := context.Background()
		mem := memstore.New()
		static := runtimecfg.NewStatic(cfg)
		c := collector.New(static, db, mem)
		go runCleanupLoop(ctx, static, db)
		go runPersistLoop(ctx, static, c)
		fatalIf(c.RunRealtime(ctx))
	case "daily":
		fs := flag.NewFlagSet("daily", flag.ExitOnError)
		cfgPath := fs.String("config", "configs/config.yaml", "config path (YAML)")
		dateStr := fs.String("date", "", "trade date (YYYY-MM-DD), default: Asia/Shanghai today")
		_ = fs.Parse(os.Args[2:])

		cfg, err := config.Load(*cfgPath)
		fatalIf(err)
		db, err := sqlite.Open(cfg.DBPath)
		fatalIf(err)
		defer db.Close()
		fatalIf(sqlite.Migrate(db))

		var d time.Time
		if *dateStr == "" {
			loc, _ := time.LoadLocation("Asia/Shanghai")
			d = time.Now().In(loc)
		} else {
			d, err = time.Parse("2006-01-02", *dateStr)
			fatalIf(err)
		}

		ctx := context.Background()
		c := collector.New(runtimecfg.NewStatic(cfg), db, memstore.New())
		fatalIf(c.RunDaily(ctx, d))
	case "web":
		fs := flag.NewFlagSet("web", flag.ExitOnError)
		cfgPath := fs.String("config", "configs/config.yaml", "config path (YAML)")
		addr := fs.String("addr", "127.0.0.1:8000", "listen address")
		_ = fs.Parse(os.Args[2:])

		mgr, err := runtimecfg.Load(*cfgPath)
		fatalIf(err)
		cfg := mgr.Get()
		db, err := sqlite.Open(cfg.DBPath)
		fatalIf(err)
		defer db.Close()
		fatalIf(sqlite.Migrate(db))

		ctx := context.Background()
		mem := memstore.New()
		c := collector.New(mgr, db, mem)
		go func() {
			if err := c.RunRealtime(ctx); err != nil {
				log.Printf("collector stopped: %v", err)
			}
		}()
		go runCleanupLoop(ctx, mgr, db)
		go runPersistLoop(ctx, mgr, c)

		srv := newWebServer(mgr)
		log.Printf("web listening on http://%s", *addr)
		fatalIf(http.ListenAndServe(*addr, srv))
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  aof init-db -config configs/config.yaml")
	fmt.Fprintln(os.Stderr, "  aof rt      -config configs/config.yaml")
	fmt.Fprintln(os.Stderr, "  aof daily   -config configs/config.yaml [-date YYYY-MM-DD]")
	fmt.Fprintln(os.Stderr, "  aof web     -config configs/config.yaml [-addr 127.0.0.1:8000]")
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

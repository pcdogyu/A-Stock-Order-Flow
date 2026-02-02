package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/config"
	"github.com/pcdogyu/A-Stock-Order-Flow/internal/collector"
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
		c := collector.New(cfg, db)
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
		c := collector.New(cfg, db)
		fatalIf(c.RunDaily(ctx, d))
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
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}


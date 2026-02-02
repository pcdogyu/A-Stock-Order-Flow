# A-Stock-Order-Flow

Go-based collector for China A-share "fund flow" signals (free-first):

- Fund flow (today net inflow): main / xl / l / m / s (Eastmoney)
- Northbound flow (沪股通/深股通): realtime snapshot (Eastmoney)
- Margin trading (融资融券): per-stock latest record (Eastmoney datacenter)
- Top list: ranked by an Eastmoney field id (default: `f62` main net inflow)

This repo is an MVP aimed at: watchlist + top榜, with daily snapshots and realtime sampling.

## Quick start (Windows / PowerShell)

1) Create config:

```powershell
Copy-Item configs/config.example.yaml configs/config.yaml
```

2) Build:

```powershell
$env:GOTOOLCHAIN='local'
go build -o .\\bin\\aof.exe .\\cmd\\aof
```

3) Init DB:

```powershell
.\bin\aof.exe init-db -config configs/config.yaml
```

4) Run realtime collector (during CN trading hours by default):

```powershell
.\bin\aof.exe rt -config configs/config.yaml
```

5) Run daily snapshot:

```powershell
.\bin\aof.exe daily -config configs/config.yaml
```

## Notes / Caveats

- "主力资金/大单/小单" are platform-derived metrics unless you compute them from Level2 ticks.
  This MVP uses the free Eastmoney fields as-is, suitable for dashboards and relative comparisons.
- CN holidays are not handled yet; daily runs are "best effort" snapshots.

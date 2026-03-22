## Docker

### Build

Build from the repository root:
```sh
docker build -f packages/docker/Dockerfile -t pelikan .
```

### Run

The default command runs `pelikan-segcache` with a config that binds both the
data port (12321) and admin port (9999) to all interfaces:
```sh
docker run -p 12321:12321 -p 9999:9999 pelikan
```

### Run a different binary

Override the command to run any of the included binaries:
```sh
docker run -p 12321:12321 pelikan pelikan-pingserver
docker run -p 12321:12321 pelikan pelikan-rds
docker run -p 12321:12321 pelikan pelikan-pingproxy
```

### Custom config

Mount your own config file:
```sh
docker run -p 12321:12321 -v /path/to/my.toml:/etc/pelikan/segcache.toml pelikan
```

Or pass it as an argument:
```sh
docker run -p 12321:12321 -v /path/to/my.toml:/tmp/my.toml pelikan pelikan-segcache /tmp/my.toml
```

### Included binaries

- `pelikan-segcache` — memcache-compatible cache with segment-based storage
- `pelikan-rds` — Redis-compatible cache
- `pelikan-pingserver` — minimal ping/pong server for testing
- `pelikan-pingproxy` — ping protocol proxy

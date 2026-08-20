# MCC DAQ Service

Implementation of a gRPC service for an MCC DAQ card intended for use with h2pcontrol.

This service should be run on a device connected to an MCC DAQ card, and enables
remote analog input and output via gRPC.

Configure the address where the service listens in the [config.toml](config.toml) file.

This project has been adapted from the [h2pcontrol-server-template](https://github.com/torbenfreise/h2pcontrol-server-template)

## Requirements

- Windows, with the MCC [InstaCal / Universal Library](https://digilent.com/reference/software/instacal/start)
  installed and the board configured as board 0. The `mcculw` package wraps the Windows-only UL DLL.
- Python 3.12+
- [uv](https://docs.astral.sh/uv/)

## Quick Start

```bash
uv run src/main.py
```

## Usage

The protobuf contract implemented by this service can be found [here](https://buf.build/beyer-labs/h2pcontrol/docs/main%3Ah2pcontrol.mccdaq.v1)

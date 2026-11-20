# modbus-connect

A robust, high-performance Modbus RTU/TCP framework for Rust with port serialization, declarative schemas, polling manager, and auto-discovery.

Designed as an architectural counterpart to the TypeScript `modbus-connect` library, `modbus-connect` provides both asynchronous (`tokio`-based) and ergonomic synchronous (blocking) APIs for industrial automation communications.

---

## Features

- **Transport Controller**: Central orchestrator managing physical connections (Serial RTU, TCP, Emulators).
- **Collision-free Port Queuing**: Per-bus priority queue (`Immediate`, `Normal`, `Background`) preventing frame collisions on shared RS-485 and TCP links.
- **High-level Modbus Client**: Standard function codes (FC 01-06, 15, 16) with automatic retry policy, timeout handling, and request-response validation.
- **Declarative Polling Manager**: Flexible task scheduler supporting recurring polling tasks, exponential backoff, pause/resume controls, and `on_data` / `on_error` callbacks.
- **Non-intrusive Traffic Sniffer**: Capture and inspect raw TX/RX frames with timestamps and hex formatting.
- **Declarative Device Schemas**: Define typed register mappings (`u16`, `i16`, `u32`, `f32`, bitmasks, ASCII strings) with automated batch reading.
- **Bus Scanner**: Auto-discovery of active Modbus slaves with adaptive timeouts and speed-optimized probe heuristics.
- **Dual Async / Blocking API**: Full async Tokio support, plus a plug-and-play synchronous blocking wrapper without manual runtime management.

---

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
modbus-connect = "0.1.0"
```

### Feature Flags

By default, all standard features are enabled (`["rtu", "tcp", "schema", "polling", "scanner", "emulator", "blocking"]`).

If you want a minimal build (e.g. TCP-only embedded without serial dependencies):

```toml
[dependencies]
modbus-connect = { version = "0.1.0", default-features = false, features = ["tcp", "polling"] }
```

Available features:
- `rtu`: Serial port communication (via `tokio-serial`).
- `tcp`: Modbus TCP communication over sockets.
- `blocking`: Ergonomic synchronous wrapper with internal runtime.
- `polling`: Recurring polling task scheduler.
- `schema`: Declarative register group mappings and data decoding.
- `scanner`: Slave ID auto-discovery scanner.
- `emulator`: Virtual slave simulation for unit tests and offline testing.
- `plugin`: Custom Modbus function code handlers.

---

## Quick Start

### 1. Synchronous (Blocking) API

Ideal for traditional synchronous apps or scripts:

```rust,no_run
use std::time::Duration;
use modbus_connect::blocking::{TransportController, PollingTask};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let controller = TransportController::new()?;

    // Register an RTU serial port
    let port = "/dev/ttyUSB0";
    controller.add_rtu(port, 57600)?;

    // Create client for slave ID 5
    let client = controller.create_client(port, 5)?;

    // Schedule a recurring polling task
    let task = PollingTask::new(
        "poll-registers",
        Duration::from_millis(500),
        move || client.read_holding_registers(0, 2),
    )
    .on_data(|data| {
        println!("Received registers: {:?}", data.as_slice());
    })
    .on_error(|err| {
        eprintln!("Polling error: {}", err);
    });

    controller.add_polling_task(port, task)?;

    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}
```

### 2. Asynchronous API (`tokio`)

```rust,no_run
use modbus_connect::TransportController;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let controller = TransportController::new();

    // Register Modbus TCP connection
    let host = "127.0.0.1:502";
    controller.add_tcp(host).await?;

    // Create client for slave ID 1
    let client = controller.create_client(host, 1, Default::default()).await?;

    // Read holding registers 0..2
    let data = client.read_holding_registers(0, 2).await?;
    println!("Register 0: {}", data[0]);

    Ok(())
}
```

---

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.

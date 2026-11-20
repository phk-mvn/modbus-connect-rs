use std::time::Duration;
use modbus_connect::blocking::{PollingTask, TransportController};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = "127.0.0.1:502";
    let slave_id = 1;

    // 1. Initialize synchronous transport controller
    let controller = TransportController::new()?;

    // 2. Register Modbus TCP connection using host:port directly
    controller.add_tcp(host)?;

    // 3. Create a client targeting slave/unit ID 1 on the TCP connection
    let client = controller.create_client(host, slave_id)?;

    // 4. Configure periodic polling task for holding registers 0..2
    let task = PollingTask::new(
        "task-read-holding-registers",
        Duration::from_millis(1000),
        move || {
            client.read_holding_registers(0, 2)
        },
    )
    .on_data(|data| {
        println!("Registers: {}", data);
        println!("Register 0: {}, Register 1: {}", data[0], data[1]);
    })
    .on_error(|err| {
        eprintln!("Error: {}", err);
    });

    // 5. Attach the polling task to the TCP transport
    controller.add_polling_task(host, task)?;

    // Keep the main thread alive to continue polling
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

use std::time::Duration;
use modbus_connect::blocking::{PollingTask, TransportController};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = "/dev/tty.usbserial-01AB5F6D";
    let baud_rate = 57600;
    let slave_id = 5;

    // 1. Initialize synchronous transport controller
    let controller = TransportController::new()?;

    // 2. Register RTU serial port using path and baud rate directly
    controller.add_rtu(port, baud_rate)?;

    // 3. Create a client targeting slave ID 5 on the registered serial port
    let client = controller.create_client(port, slave_id)?;

    // 4. Configure periodic polling task for holding registers 0..2
    let task = PollingTask::new(
        "task-read-holding-registers",
        Duration::from_millis(500),
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

    // 5. Attach the polling task to the transport
    controller.add_polling_task(port, task)?;

    // Keep the main thread alive to continue polling
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

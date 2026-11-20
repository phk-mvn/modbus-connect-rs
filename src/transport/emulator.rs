//! In-memory Modbus slave emulator for unit testing and offline simulation.
//!
//! [`SlaveEmulator`] acts as a full-featured Modbus RTU or TCP server held in RAM.
//! It stores coils, discrete inputs, holding registers, and input registers in async-safe
//! hash maps and responds to standard Modbus function codes (FC 01, 02, 03, 04, 05, 06, 15, 16).

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, RwLock};

use crate::core::crc::{check_crc, crc16};
use crate::core::framing::{FramingKind, TcpFramer};
use crate::core::pdu::{pack_coils, unpack_coils, Request, Response};
use crate::error::{ExceptionCode, ModbusError, Result};
use crate::transport::traits::Transport;

/// Target register table type for simulation in [`SlaveEmulator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmulatorRegisterType {
    /// 16-bit Read/Write Holding Registers (FC 03 / 06 / 16).
    Holding,
    /// 16-bit Read-Only Input Registers (FC 04).
    Input,
    /// 1-bit Read/Write Coils (FC 01 / 05 / 15).
    Coil,
    /// 1-bit Read-Only Discrete Inputs (FC 02).
    Discrete,
}

/// Parameters for automated periodic sensor value simulation.
#[derive(Debug, Clone)]
pub struct InfinityChangeParams {
    /// Target register category.
    pub register_type: EmulatorRegisterType,
    /// Target register address.
    pub address: u16,
    /// Range `(min, max)` within which values fluctuate.
    pub range: (u16, u16),
    /// Simulation update interval.
    pub interval: Duration,
    /// Increment/decrement step (default: 1).
    pub step: i16,
}

impl InfinityChangeParams {
    /// Creates a new simulation parameter set.
    pub fn new(
        register_type: EmulatorRegisterType,
        address: u16,
        range: (u16, u16),
        interval: Duration,
    ) -> Self {
        Self {
            register_type,
            address,
            range,
            interval,
            step: 1,
        }
    }

    /// Sets the change step.
    pub fn step(mut self, step: i16) -> Self {
        self.step = step;
        self
    }
}

/// In-memory Modbus slave device that implements [`Transport`].
///
/// Enables fast, deterministic unit and integration tests without physical serial ports
/// or network sockets.
///
/// # Example
///
/// ```
/// use modbus_connect::transport::emulator::SlaveEmulator;
/// use modbus_connect::core::framing::FramingKind;
///
/// # #[tokio::main]
/// # async fn main() {
/// let emulator = SlaveEmulator::new(1, FramingKind::Rtu);
/// emulator.set_holding_register(100, 1234).await;
/// assert_eq!(emulator.get_holding_register(100).await, Some(1234));
/// # }
/// ```
#[derive(Clone)]
pub struct SlaveEmulator {
    slave_id: u8,
    framing: FramingKind,
    coils: Arc<RwLock<HashMap<u16, bool>>>,
    discrete_inputs: Arc<RwLock<HashMap<u16, bool>>>,
    holding_registers: Arc<RwLock<HashMap<u16, u16>>>,
    input_registers: Arc<RwLock<HashMap<u16, u16>>>,
    exceptions: Arc<RwLock<HashMap<(u8, u16), ExceptionCode>>>,
    response_latency: Arc<RwLock<Duration>>,
    infinity_cancels: Arc<RwLock<HashMap<(EmulatorRegisterType, u16), oneshot::Sender<()>>>>,
}

impl SlaveEmulator {
    /// Creates a new emulator instance configured with the given unit/slave ID and framing mode.
    pub fn new(slave_id: u8, framing: FramingKind) -> Self {
        Self {
            slave_id,
            framing,
            coils: Arc::new(RwLock::new(HashMap::new())),
            discrete_inputs: Arc::new(RwLock::new(HashMap::new())),
            holding_registers: Arc::new(RwLock::new(HashMap::new())),
            input_registers: Arc::new(RwLock::new(HashMap::new())),
            exceptions: Arc::new(RwLock::new(HashMap::new())),
            response_latency: Arc::new(RwLock::new(Duration::ZERO)),
            infinity_cancels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Sets an artificial response delay to simulate network latency or slow slave response times.
    pub async fn set_response_latency(&self, latency: Duration) {
        let mut l = self.response_latency.write().await;
        *l = latency;
    }

    /// Returns the currently configured simulated response latency.
    pub async fn response_latency(&self) -> Duration {
        *self.response_latency.read().await
    }

    /// Configures a simulated Modbus exception for a specific function code and address.
    pub async fn set_exception(&self, function_code: u8, address: u16, exception_code: ExceptionCode) {
        let mut map = self.exceptions.write().await;
        map.insert((function_code, address), exception_code);
    }

    /// Clears any configured exception for a specific function code and address.
    pub async fn clear_exception(&self, function_code: u8, address: u16) {
        let mut map = self.exceptions.write().await;
        map.remove(&(function_code, address));
    }

    /// Clears all stored registers, coils, active simulations, and configured exceptions.
    pub async fn clear_all(&self) {
        self.stop_all_infinity_change().await;
        self.coils.write().await.clear();
        self.discrete_inputs.write().await.clear();
        self.holding_registers.write().await.clear();
        self.input_registers.write().await.clear();
        self.exceptions.write().await.clear();
    }

    /// Starts a recurring background simulation task periodically fluctuating register values.
    pub async fn infinity_change(&self, params: InfinityChangeParams) {
        let (cancel_tx, mut cancel_rx) = oneshot::channel();
        {
            let mut map = self.infinity_cancels.write().await;
            if let Some(prev) = map.insert((params.register_type, params.address), cancel_tx) {
                let _ = prev.send(());
            }
        }

        let coils = self.coils.clone();
        let discrete_inputs = self.discrete_inputs.clone();
        let holding_registers = self.holding_registers.clone();
        let input_registers = self.input_registers.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(params.interval);
            let (min, max) = params.range;
            let mut current = min;
            let mut direction_up = true;

            loop {
                tokio::select! {
                    _ = &mut cancel_rx => {
                        break;
                    }
                    _ = ticker.tick() => {
                        match params.register_type {
                            EmulatorRegisterType::Holding => {
                                holding_registers.write().await.insert(params.address, current);
                            }
                            EmulatorRegisterType::Input => {
                                input_registers.write().await.insert(params.address, current);
                            }
                            EmulatorRegisterType::Coil => {
                                coils.write().await.insert(params.address, current != 0);
                            }
                            EmulatorRegisterType::Discrete => {
                                discrete_inputs.write().await.insert(params.address, current != 0);
                            }
                        }

                        if params.step > 0 {
                            let step = params.step.unsigned_abs();
                            if direction_up {
                                if current + step >= max {
                                    current = max;
                                    direction_up = false;
                                } else {
                                    current += step;
                                }
                            } else {
                                if current <= min + step {
                                    current = min;
                                    direction_up = true;
                                } else {
                                    current -= step;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    /// Stops an active `infinity_change` simulation task on the given register.
    pub async fn stop_infinity_change(&self, register_type: EmulatorRegisterType, address: u16) {
        let mut map = self.infinity_cancels.write().await;
        if let Some(tx) = map.remove(&(register_type, address)) {
            let _ = tx.send(());
        }
    }

    /// Stops all currently active `infinity_change` simulation tasks.
    pub async fn stop_all_infinity_change(&self) {
        let mut map = self.infinity_cancels.write().await;
        for (_, tx) in map.drain() {
            let _ = tx.send(());
        }
    }

    /// Sets the value of a 16-bit holding register (FC 03 / FC 06 / FC 16).
    pub async fn set_holding_register(&self, address: u16, value: u16) {
        let mut map = self.holding_registers.write().await;
        map.insert(address, value);
    }

    /// Reads the current value of a holding register in memory, if previously set.
    pub async fn get_holding_register(&self, address: u16) -> Option<u16> {
        let map = self.holding_registers.read().await;
        map.get(&address).copied()
    }

    /// Sets the value of a 16-bit input register (FC 04).
    pub async fn set_input_register(&self, address: u16, value: u16) {
        let mut map = self.input_registers.write().await;
        map.insert(address, value);
    }

    /// Sets the boolean state of a coil (FC 01 / FC 05 / FC 15).
    pub async fn set_coil(&self, address: u16, value: bool) {
        let mut map = self.coils.write().await;
        map.insert(address, value);
    }

    /// Sets the boolean state of a discrete input (FC 02).
    pub async fn set_discrete_input(&self, address: u16, value: bool) {
        let mut map = self.discrete_inputs.write().await;
        map.insert(address, value);
    }

    /// Internal handler dispatching a parsed [`Request`] against the internal memory maps.
    async fn process_request(&self, request: Request) -> Result<Response> {
        let (fc, addr) = match &request {
            Request::ReadCoils { address, .. } => (0x01, *address),
            Request::ReadDiscreteInputs { address, .. } => (0x02, *address),
            Request::ReadHoldingRegisters { address, .. } => (0x03, *address),
            Request::ReadInputRegisters { address, .. } => (0x04, *address),
            Request::WriteSingleCoil { address, .. } => (0x05, *address),
            Request::WriteSingleRegister { address, .. } => (0x06, *address),
            Request::WriteMultipleCoils { address, .. } => (0x0F, *address),
            Request::WriteMultipleRegisters { address, .. } => (0x10, *address),
        };

        if let Some(&exc) = self.exceptions.read().await.get(&(fc, addr)) {
            return Err(ModbusError::Exception(exc));
        }

        match request {
            Request::ReadHoldingRegisters { address, quantity } => {
                let map = self.holding_registers.read().await;
                let mut regs = Vec::with_capacity(quantity as usize);
                for i in 0..quantity {
                    regs.push(map.get(&(address + i)).copied().unwrap_or(0));
                }
                Ok(Response::ReadHoldingRegisters(regs))
            }
            Request::ReadInputRegisters { address, quantity } => {
                let map = self.input_registers.read().await;
                let mut regs = Vec::with_capacity(quantity as usize);
                for i in 0..quantity {
                    regs.push(map.get(&(address + i)).copied().unwrap_or(0));
                }
                Ok(Response::ReadInputRegisters(regs))
            }
            Request::ReadCoils { address, quantity } => {
                let map = self.coils.read().await;
                let mut values = Vec::with_capacity(quantity as usize);
                for i in 0..quantity {
                    values.push(map.get(&(address + i)).copied().unwrap_or(false));
                }
                Ok(Response::ReadCoils(values))
            }
            Request::ReadDiscreteInputs { address, quantity } => {
                let map = self.discrete_inputs.read().await;
                let mut values = Vec::with_capacity(quantity as usize);
                for i in 0..quantity {
                    values.push(map.get(&(address + i)).copied().unwrap_or(false));
                }
                Ok(Response::ReadDiscreteInputs(values))
            }
            Request::WriteSingleRegister { address, value } => {
                let mut map = self.holding_registers.write().await;
                map.insert(address, value);
                Ok(Response::WriteSingleRegister { address, value })
            }
            Request::WriteMultipleRegisters { address, values } => {
                let mut map = self.holding_registers.write().await;
                for (i, &val) in values.iter().enumerate() {
                    map.insert(address + i as u16, val);
                }
                Ok(Response::WriteMultipleRegisters {
                    address,
                    quantity: values.len() as u16,
                })
            }
            Request::WriteSingleCoil { address, value } => {
                let mut map = self.coils.write().await;
                map.insert(address, value);
                Ok(Response::WriteSingleCoil { address, value })
            }
            Request::WriteMultipleCoils { address, values } => {
                let mut map = self.coils.write().await;
                for (i, &val) in values.iter().enumerate() {
                    map.insert(address + i as u16, val);
                }
                Ok(Response::WriteMultipleCoils {
                    address,
                    quantity: values.len() as u16,
                })
            }
        }
    }
}

#[async_trait]
impl Transport for SlaveEmulator {
    /// Exchanges an emulated frame according to the configured [`FramingKind`].
    ///
    /// Validates frame integrity, matches slave ID, decodes the PDU, processes it in memory,
    /// and formats an exact ADU response with correct CRC16 or MBAP headers.
    async fn exchange(&mut self, request_frame: &[u8], timeout: Duration) -> Result<Vec<u8>> {
        let latency = *self.response_latency.read().await;
        if latency > Duration::ZERO {
            tokio::time::sleep(latency).await;
        }

        match self.framing {
            FramingKind::Rtu => {
                if request_frame.len() < 4 {
                    return Err(ModbusError::BufferUnderrun { expected: 4, actual: request_frame.len() });
                }
                if !check_crc(request_frame) {
                    return Err(ModbusError::InvalidFrame("CRC error in request".into()));
                }
                let slave = request_frame[0];
                if slave != self.slave_id {
                    return Err(ModbusError::Timeout(timeout));
                }
                let pdu = &request_frame[1..request_frame.len() - 2];
                let fc = pdu[0];
                let req = decode_request_pdu(pdu)?;
                let resp_pdu = match self.process_request(req).await {
                    Ok(resp) => encode_response_pdu(&resp),
                    Err(ModbusError::Exception(exc)) => vec![fc | 0x80, u8::from(exc)],
                    Err(e) => return Err(e),
                };
                let mut response_frame = Vec::with_capacity(1 + resp_pdu.len() + 2);
                response_frame.push(self.slave_id);
                response_frame.extend_from_slice(&resp_pdu);
                let crc = crc16(&response_frame);
                response_frame.push((crc & 0xFF) as u8);
                response_frame.push((crc >> 8) as u8);
                Ok(response_frame)
            }
            FramingKind::Tcp => {
                if request_frame.len() < TcpFramer::MBAP_HEADER_LEN {
                    return Err(ModbusError::BufferUnderrun {
                        expected: TcpFramer::MBAP_HEADER_LEN,
                        actual: request_frame.len(),
                    });
                }
                let tx_id = u16::from_be_bytes([request_frame[0], request_frame[1]]);
                let unit_id = request_frame[6];
                if unit_id != self.slave_id {
                    return Err(ModbusError::Timeout(timeout));
                }
                let pdu = &request_frame[TcpFramer::MBAP_HEADER_LEN..];
                let fc = pdu[0];
                let req = decode_request_pdu(pdu)?;
                let resp_pdu = match self.process_request(req).await {
                    Ok(resp) => encode_response_pdu(&resp),
                    Err(ModbusError::Exception(exc)) => vec![fc | 0x80, u8::from(exc)],
                    Err(e) => return Err(e),
                };
                let length = (resp_pdu.len() + 1) as u16;
                let mut response_frame = Vec::with_capacity(7 + resp_pdu.len());
                response_frame.extend_from_slice(&tx_id.to_be_bytes());
                response_frame.extend_from_slice(&0u16.to_be_bytes());
                response_frame.extend_from_slice(&length.to_be_bytes());
                response_frame.push(unit_id);
                response_frame.extend_from_slice(&resp_pdu);
                Ok(response_frame)
            }
        }
    }

    async fn write_frame(&mut self, request_frame: &[u8]) -> Result<()> {
        let _ = self.exchange(request_frame, Duration::from_millis(100)).await;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> { Ok(()) }
    async fn close(&mut self) -> Result<()> { Ok(()) }
    fn is_open(&self) -> bool { true }
}

fn decode_request_pdu(pdu: &[u8]) -> Result<Request> {
    if pdu.is_empty() {
        return Err(ModbusError::BufferUnderrun { expected: 1, actual: 0 });
    }
    let fc = pdu[0];
    let payload = &pdu[1..];
    match fc {
        0x01 => {
            if payload.len() < 4 { return Err(ModbusError::BufferUnderrun { expected: 5, actual: pdu.len() }); }
            let address = u16::from_be_bytes([payload[0], payload[1]]);
            let quantity = u16::from_be_bytes([payload[2], payload[3]]);
            Ok(Request::ReadCoils { address, quantity })
        }
        0x02 => {
            if payload.len() < 4 { return Err(ModbusError::BufferUnderrun { expected: 5, actual: pdu.len() }); }
            let address = u16::from_be_bytes([payload[0], payload[1]]);
            let quantity = u16::from_be_bytes([payload[2], payload[3]]);
            Ok(Request::ReadDiscreteInputs { address, quantity })
        }
        0x03 => {
            if payload.len() < 4 { return Err(ModbusError::BufferUnderrun { expected: 5, actual: pdu.len() }); }
            let address = u16::from_be_bytes([payload[0], payload[1]]);
            let quantity = u16::from_be_bytes([payload[2], payload[3]]);
            Ok(Request::ReadHoldingRegisters { address, quantity })
        }
        0x04 => {
            if payload.len() < 4 { return Err(ModbusError::BufferUnderrun { expected: 5, actual: pdu.len() }); }
            let address = u16::from_be_bytes([payload[0], payload[1]]);
            let quantity = u16::from_be_bytes([payload[2], payload[3]]);
            Ok(Request::ReadInputRegisters { address, quantity })
        }
        0x05 => {
            if payload.len() < 4 { return Err(ModbusError::BufferUnderrun { expected: 5, actual: pdu.len() }); }
            let address = u16::from_be_bytes([payload[0], payload[1]]);
            let raw_val = u16::from_be_bytes([payload[2], payload[3]]);
            Ok(Request::WriteSingleCoil { address, value: raw_val == 0xFF00 })
        }
        0x06 => {
            if payload.len() < 4 { return Err(ModbusError::BufferUnderrun { expected: 5, actual: pdu.len() }); }
            let address = u16::from_be_bytes([payload[0], payload[1]]);
            let value = u16::from_be_bytes([payload[2], payload[3]]);
            Ok(Request::WriteSingleRegister { address, value })
        }
        0x0F => {
            if payload.len() < 5 { return Err(ModbusError::BufferUnderrun { expected: 6, actual: pdu.len() }); }
            let address = u16::from_be_bytes([payload[0], payload[1]]);
            let quantity = u16::from_be_bytes([payload[2], payload[3]]) as usize;
            let byte_count = payload[4] as usize;
            let coils_bytes = &payload[5..];
            if coils_bytes.len() < byte_count {
                return Err(ModbusError::BufferUnderrun { expected: 5 + byte_count, actual: pdu.len() });
            }
            let values = unpack_coils(coils_bytes, quantity);
            Ok(Request::WriteMultipleCoils { address, values })
        }
        0x10 => {
            if payload.len() < 5 { return Err(ModbusError::BufferUnderrun { expected: 6, actual: pdu.len() }); }
            let address = u16::from_be_bytes([payload[0], payload[1]]);
            let quantity = u16::from_be_bytes([payload[2], payload[3]]) as usize;
            let byte_count = payload[4] as usize;
            let reg_bytes = &payload[5..];
            if reg_bytes.len() < byte_count {
                return Err(ModbusError::BufferUnderrun { expected: 5 + byte_count, actual: pdu.len() });
            }
            let mut values = Vec::with_capacity(quantity);
            for chunk in reg_bytes.chunks_exact(2).take(quantity) {
                values.push(u16::from_be_bytes([chunk[0], chunk[1]]));
            }
            Ok(Request::WriteMultipleRegisters { address, values })
        }
        _ => Err(ModbusError::Exception(ExceptionCode::IllegalFunction)),
    }
}

fn encode_response_pdu(resp: &Response) -> Vec<u8> {
    let mut out = Vec::new();
    match resp {
        Response::ReadCoils(coils) => {
            out.push(0x01);
            let packed = pack_coils(coils);
            out.push(packed.len() as u8);
            out.extend_from_slice(&packed);
        }
        Response::ReadDiscreteInputs(inputs) => {
            out.push(0x02);
            let packed = pack_coils(inputs);
            out.push(packed.len() as u8);
            out.extend_from_slice(&packed);
        }
        Response::ReadHoldingRegisters(regs) => {
            out.push(0x03);
            out.push((regs.len() * 2) as u8);
            for r in regs {
                out.extend_from_slice(&r.to_be_bytes());
            }
        }
        Response::ReadInputRegisters(regs) => {
            out.push(0x04);
            out.push((regs.len() * 2) as u8);
            for r in regs {
                out.extend_from_slice(&r.to_be_bytes());
            }
        }
        Response::WriteSingleCoil { address, value } => {
            out.push(0x05);
            out.extend_from_slice(&address.to_be_bytes());
            let raw: u16 = if *value { 0xFF00 } else { 0x0000 };
            out.extend_from_slice(&raw.to_be_bytes());
        }
        Response::WriteSingleRegister { address, value } => {
            out.push(0x06);
            out.extend_from_slice(&address.to_be_bytes());
            out.extend_from_slice(&value.to_be_bytes());
        }
        Response::WriteMultipleCoils { address, quantity } => {
            out.push(0x0F);
            out.extend_from_slice(&address.to_be_bytes());
            out.extend_from_slice(&quantity.to_be_bytes());
        }
        Response::WriteMultipleRegisters { address, quantity } => {
            out.push(0x10);
            out.extend_from_slice(&address.to_be_bytes());
            out.extend_from_slice(&quantity.to_be_bytes());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::client::{ClientOptions, ModbusClient};
    use crate::transport::queue::PortQueueHandle;

    #[tokio::test]
    async fn test_emulator_read_write_cycle() {
        let emulator = SlaveEmulator::new(1, FramingKind::Rtu);
        emulator.set_holding_register(10, 42).await;
        let queue = PortQueueHandle::new(emulator.clone(), 10);
        let client = ModbusClient::new(1, FramingKind::Rtu, queue, ClientOptions::default()).unwrap();
        let data = client.read_holding_registers(10, 1).await.unwrap();
        assert_eq!(data.get_u16(0).unwrap(), 42);
        client.write_single_register(10, 99).await.unwrap();
        assert_eq!(emulator.get_holding_register(10).await, Some(99));
    }

    #[tokio::test]
    async fn test_emulator_exception_and_simulation() {
        let emulator = SlaveEmulator::new(1, FramingKind::Rtu);
        emulator.set_response_latency(Duration::from_millis(5)).await;
        assert_eq!(emulator.response_latency().await, Duration::from_millis(5));

        // Inject exception for FC 03 at address 100
        emulator.set_exception(0x03, 100, ExceptionCode::IllegalDataAddress).await;

        let queue = PortQueueHandle::new(emulator.clone(), 10);
        let client = ModbusClient::new(1, FramingKind::Rtu, queue, ClientOptions::default()).unwrap();

        let err = client.read_holding_registers(100, 1).await.unwrap_err();
        assert!(matches!(err, ModbusError::Exception(ExceptionCode::IllegalDataAddress)));

        // Test infinity_change sensor simulation
        emulator.infinity_change(InfinityChangeParams::new(
            EmulatorRegisterType::Holding,
            50,
            (100, 200),
            Duration::from_millis(10),
        )).await;

        tokio::time::sleep(Duration::from_millis(35)).await;
        let val = emulator.get_holding_register(50).await.unwrap();
        assert!(val >= 100 && val <= 200);

        emulator.stop_all_infinity_change().await;
    }
}

use crate::prelude::*;
use std::net::{IpAddr, ToSocketAddrs};

pub fn parse_ip(value: &str) -> Result<IpAddr> {
    match value.parse::<IpAddr>() {
        Ok(ip) => Ok(ip),
        Err(_) => {
            let mut socket_addresses = (value, 80u16).to_socket_addrs().context("unknown dns")?;
            let socket = socket_addresses.next().context("unknown dns")?;
            Ok(socket.ip())
        }
    }
}

use prost::Message;

/// Stream type tag — first byte on every QUIC bidi stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamTag {
    Control = 0x00,
    ChunkRequest = 0x01,
}

impl TryFrom<u8> for StreamTag {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0x00 => Ok(Self::Control),
            0x01 => Ok(Self::ChunkRequest),
            other => Err(other),
        }
    }
}

/// Encode a protobuf message as length-delimited bytes.
pub fn encode_msg<M: Message>(msg: &M) -> Vec<u8> {
    msg.encode_length_delimited_to_vec()
}

/// Decode a length-delimited protobuf message from bytes.
pub fn decode_msg<M: Message + Default>(buf: &[u8]) -> Result<M, prost::DecodeError> {
    M::decode_length_delimited(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto;

    #[test]
    fn roundtrip_encode_decode() {
        let msg = proto::Handshake {
            satellite_id: "sat-1".into(),
            protocol_version: 1,
        };
        let encoded = encode_msg(&msg);
        let decoded: proto::Handshake = decode_msg(&encoded).unwrap();
        assert_eq!(decoded.satellite_id, "sat-1");
        assert_eq!(decoded.protocol_version, 1);
    }

    #[test]
    fn stream_tag_roundtrip() {
        assert_eq!(StreamTag::Control as u8, 0x00);
        assert_eq!(StreamTag::ChunkRequest as u8, 0x01);
        assert_eq!(StreamTag::try_from(0x00).unwrap(), StreamTag::Control);
        assert_eq!(StreamTag::try_from(0x01).unwrap(), StreamTag::ChunkRequest);
        assert!(StreamTag::try_from(0xFF).is_err());
    }
}

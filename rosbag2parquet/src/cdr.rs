use anyhow::{bail, Result};
use byteorder::{BigEndian, ByteOrder, LittleEndian};

const CDR_HEADER_SIZE: usize = 4;

#[derive(Copy, Clone, PartialEq, Debug, Default)]
pub enum Endianness {
    #[default]
    BigEndian,
    LittleEndian,
}

impl Endianness {
    fn from_cdr_header(data: &[u8]) -> Self {
        match data[1] {
            0x00 => Self::BigEndian,
            _ => Self::LittleEndian,
        }
    }
}

#[derive(Default, Debug)]
pub struct CdrDeserializer<'a> {
    data: &'a [u8],
    byte_order: Endianness,
    position: usize,
}

impl<'a> CdrDeserializer<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let byte_order = Endianness::from_cdr_header(data);

        Self {
            data,
            byte_order,
            position: CDR_HEADER_SIZE,
        }
    }

    #[inline]
    pub fn align_to(&mut self, count: usize) {
        let modulo = (self.position - CDR_HEADER_SIZE) % count;
        if modulo != 0 {
            self.position += count - modulo;
        }
    }

    #[inline]
    pub fn next_bytes(&mut self, count: usize) -> Result<&'a [u8]> {
        if self.position + count > self.data.len() {
            bail!(
                "Buffer underrun: attempted to read {} bytes at position {}, but only {} bytes available",
                count,
                self.position,
                self.data.len() - self.position
            );
        }
        self.position += count;
        Ok(&self.data[self.position - count..self.position])
    }

    #[inline]
    pub fn read_sequence_length(&mut self) -> Result<u32> {
        self.align_to(4);
        let header = self.next_bytes(4)?;
        Ok(match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(header),
            Endianness::LittleEndian => LittleEndian::read_u32(header),
        })
    }

    pub fn deserialize_f64(&mut self) -> Result<f64> {
        self.align_to(8);
        let bytes = self.next_bytes(8)?;
        Ok(match self.byte_order {
            Endianness::BigEndian => BigEndian::read_f64(bytes),
            Endianness::LittleEndian => LittleEndian::read_f64(bytes),
        })
    }

    pub fn deserialize_f32(&mut self) -> Result<f32> {
        self.align_to(4);
        let bytes = self.next_bytes(4)?;
        Ok(match self.byte_order {
            Endianness::BigEndian => BigEndian::read_f32(bytes),
            Endianness::LittleEndian => LittleEndian::read_f32(bytes),
        })
    }

    pub fn deserialize_bool(&mut self) -> Result<bool> {
        let bytes = self.next_bytes(1)?;
        Ok(bytes[0] == 0x01)
    }

    pub fn deserialize_i8(&mut self) -> Result<i8> {
        let bytes = self.next_bytes(1)?;
        Ok(bytes[0] as i8)
    }

    pub fn deserialize_u8(&mut self) -> Result<u8> {
        let bytes = self.next_bytes(1)?;
        Ok(bytes[0])
    }

    pub fn deserialize_char(&mut self) -> Result<char> {
        let byte = self.next_bytes(1)?[0];
        Ok(byte as char)
    }

    pub fn deserialize_i16(&mut self) -> Result<i16> {
        self.align_to(2);
        let bytes = self.next_bytes(2)?;
        Ok(match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i16(bytes),
            Endianness::LittleEndian => LittleEndian::read_i16(bytes),
        })
    }

    pub fn deserialize_u16(&mut self) -> Result<u16> {
        self.align_to(2);
        let bytes = self.next_bytes(2)?;
        Ok(match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u16(bytes),
            Endianness::LittleEndian => LittleEndian::read_u16(bytes),
        })
    }

    pub fn deserialize_i32(&mut self) -> Result<i32> {
        self.align_to(4);
        let bytes = self.next_bytes(4)?;
        Ok(match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i32(bytes),
            Endianness::LittleEndian => LittleEndian::read_i32(bytes),
        })
    }

    pub fn deserialize_u32(&mut self) -> Result<u32> {
        self.align_to(4);
        let bytes = self.next_bytes(4)?;
        Ok(match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(bytes),
            Endianness::LittleEndian => LittleEndian::read_u32(bytes),
        })
    }

    pub fn deserialize_i64(&mut self) -> Result<i64> {
        self.align_to(8);
        let bytes = self.next_bytes(8)?;
        Ok(match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i64(bytes),
            Endianness::LittleEndian => LittleEndian::read_i64(bytes),
        })
    }

    pub fn deserialize_u64(&mut self) -> Result<u64> {
        self.align_to(8);
        let bytes = self.next_bytes(8)?;
        Ok(match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u64(bytes),
            Endianness::LittleEndian => LittleEndian::read_u64(bytes),
        })
    }

    pub fn deserialize_string(&mut self) -> Result<String> {
        self.align_to(4);
        let header = self.next_bytes(4)?;
        let byte_length = match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(header),
            Endianness::LittleEndian => LittleEndian::read_u32(header),
        };
        let bytes = self.next_bytes(byte_length as usize)?;
        let bytes_without_null = match bytes.split_last() {
            None => bytes,
            Some((_null_char, contents)) => contents,
        };
        std::str::from_utf8(bytes_without_null)
            .map(|s| s.to_string())
            .map_err(|err| {
                anyhow::anyhow!(
                    "Invalid UTF-8 string at position {}: {}",
                    self.position - byte_length as usize,
                    err
                )
            })
    }
}

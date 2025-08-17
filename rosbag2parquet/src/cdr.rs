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
    pub fn next_bytes(&mut self, count: usize) -> &'a [u8] {
        self.position += count;
        &self.data[self.position - count..self.position]
    }

    #[inline]
    pub fn read_sequence_length(&mut self) -> u32 {
        self.align_to(4);
        let header = self.next_bytes(4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(header),
            Endianness::LittleEndian => LittleEndian::read_u32(header),
        }
    }

    pub fn deserialize_f64(&mut self) -> f64 {
        self.align_to(8);
        let bytes = self.next_bytes(8);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_f64(bytes),
            Endianness::LittleEndian => LittleEndian::read_f64(bytes),
        }
    }

    pub fn deserialize_f32(&mut self) -> f32 {
        self.align_to(4);
        let bytes = self.next_bytes(4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_f32(bytes),
            Endianness::LittleEndian => LittleEndian::read_f32(bytes),
        }
    }

    pub fn deserialize_bool(&mut self) -> bool {
        let bytes = self.next_bytes(1);
        bytes[0] == 0x01
    }

    pub fn deserialize_i8(&mut self) -> i8 {
        let bytes = self.next_bytes(1);
        bytes[0] as i8
    }

    pub fn deserialize_u8(&mut self) -> u8 {
        let bytes = self.next_bytes(1);
        bytes[0]
    }

    pub fn deserialize_char(&mut self) -> char {
        let byte = self.next_bytes(1)[0];
        byte as char
    }

    pub fn deserialize_i16(&mut self) -> i16 {
        self.align_to(2);
        let bytes = self.next_bytes(2);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i16(bytes),
            Endianness::LittleEndian => LittleEndian::read_i16(bytes),
        }
    }

    pub fn deserialize_u16(&mut self) -> u16 {
        self.align_to(2);
        let bytes = self.next_bytes(2);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u16(bytes),
            Endianness::LittleEndian => LittleEndian::read_u16(bytes),
        }
    }

    pub fn deserialize_i32(&mut self) -> i32 {
        self.align_to(4);
        let bytes = self.next_bytes(4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i32(bytes),
            Endianness::LittleEndian => LittleEndian::read_i32(bytes),
        }
    }

    pub fn deserialize_u32(&mut self) -> u32 {
        self.align_to(4);
        let bytes = self.next_bytes(4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(bytes),
            Endianness::LittleEndian => LittleEndian::read_u32(bytes),
        }
    }

    pub fn deserialize_i64(&mut self) -> i64 {
        self.align_to(8);
        let bytes = self.next_bytes(8);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i64(bytes),
            Endianness::LittleEndian => LittleEndian::read_i64(bytes),
        }
    }

    pub fn deserialize_u64(&mut self) -> u64 {
        self.align_to(8);
        let bytes = self.next_bytes(8);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u64(bytes),
            Endianness::LittleEndian => LittleEndian::read_u64(bytes),
        }
    }

    pub fn deserialize_string(&mut self) -> String {
        self.align_to(4);
        let header = self.next_bytes(4);
        let byte_length = match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(header),
            Endianness::LittleEndian => LittleEndian::read_u32(header),
        };
        let bytes = self.next_bytes(byte_length as usize);
        let bytes_without_null = match bytes.split_last() {
            None => bytes,
            Some((_null_char, contents)) => contents,
        };
        std::str::from_utf8(bytes_without_null)
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to decode UTF-8 string from CDR data at position {}. Error: {}",
                    self.position - byte_length as usize,
                    err
                )
            })
            .to_string()
    }
}

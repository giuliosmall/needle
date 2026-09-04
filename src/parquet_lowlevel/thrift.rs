//! Minimal Thrift compact-protocol encoder for the Parquet structs we emit.
//!
//! Spec: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
//! Parquet: all metadata (page headers, FileMetaData, OffsetIndex) is TCompactProtocol.

#![allow(dead_code)]

const STOP: u8 = 0;
const BOOL_TRUE: u8 = 1;
const BOOL_FALSE: u8 = 2;
const I16: u8 = 4;
const I32: u8 = 5;
const I64: u8 = 6;
const BINARY: u8 = 8;
const LIST: u8 = 9;
const STRUCT: u8 = 12;

const ELEM_BOOL: u8 = 2;
const ELEM_I32: u8 = 5;
const ELEM_I64: u8 = 6;
const ELEM_BINARY: u8 = 8;
const ELEM_STRUCT: u8 = 12;

/// Physical types (parquet.thrift `Type`)
pub const TYPE_INT64: i32 = 2;
pub const TYPE_BYTE_ARRAY: i32 = 6;

/// `Repetition`
pub const REP_REQUIRED: i32 = 0;

/// `ConvertedType`
pub const CONV_UTF8: i32 = 0;
pub const CONV_TIMESTAMP_MILLIS: i32 = 9;

/// `PageType`
pub const PAGE_DATA: i32 = 0;

/// `Encoding`
pub const ENC_PLAIN: i32 = 0;
pub const ENC_RLE: i32 = 3;

/// `CompressionCodec`
pub const CODEC_UNCOMPRESSED: i32 = 0;
pub const CODEC_ZSTD: i32 = 6;

pub struct Compact {
    pub buf: Vec<u8>,
    last: i16,
}

impl Compact {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            last: 0,
        }
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    fn field_begin(&mut self, ty: u8, id: i16) {
        let delta = id.wrapping_sub(self.last);
        if delta > 0 && delta <= 0xf {
            self.buf.push((delta as u8) << 4 | ty);
        } else {
            self.buf.push(ty);
            self.write_i16_raw(id);
        }
        self.last = id;
    }

    fn write_vlq(&mut self, mut v: u64) {
        while v > 0x7f {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }

    fn write_zigzag(&mut self, val: i64) {
        let s = (val < 0) as i64;
        self.write_vlq((((val ^ -s) << 1) + s) as u64);
    }

    fn write_i16_raw(&mut self, val: i16) {
        self.write_zigzag(val as i64);
    }

    fn write_i32_raw(&mut self, val: i32) {
        self.write_zigzag(val as i64);
    }

    fn write_i64_raw(&mut self, val: i64) {
        self.write_zigzag(val);
    }

    fn stop(&mut self) {
        self.buf.push(STOP);
    }

    pub fn field_bool(&mut self, id: i16, val: bool) {
        self.field_begin(if val { BOOL_TRUE } else { BOOL_FALSE }, id);
    }

    pub fn field_i16(&mut self, id: i16, val: i16) {
        self.field_begin(I16, id);
        self.write_i16_raw(val);
    }

    pub fn field_i32(&mut self, id: i16, val: i32) {
        self.field_begin(I32, id);
        self.write_i32_raw(val);
    }

    pub fn field_i64(&mut self, id: i16, val: i64) {
        self.field_begin(I64, id);
        self.write_i64_raw(val);
    }

    pub fn field_str(&mut self, id: i16, s: &str) {
        self.field_begin(BINARY, id);
        self.write_vlq(s.len() as u64);
        self.buf.extend_from_slice(s.as_bytes());
    }

    pub fn field_binary(&mut self, id: i16, b: &[u8]) {
        self.field_begin(BINARY, id);
        self.write_vlq(b.len() as u64);
        self.buf.extend_from_slice(b);
    }

    /// Begin a struct-valued field; returns a guard that writes STOP and restores last-id.
    pub fn struct_field<F: FnOnce(&mut Self)>(&mut self, id: i16, body: F) {
        self.field_begin(STRUCT, id);
        let parent_last = self.last;
        self.last = 0;
        body(self);
        self.stop();
        self.last = parent_last;
    }

    pub fn list_struct_field<T, F>(&mut self, id: i16, items: &[T], mut body: F)
    where
        F: FnMut(&mut Self, &T),
    {
        self.field_begin(LIST, id);
        self.list_begin(ELEM_STRUCT, items.len());
        for item in items {
            let parent_last = self.last;
            self.last = 0;
            body(self, item);
            self.stop();
            self.last = parent_last;
        }
    }

    pub fn list_i32_field(&mut self, id: i16, items: &[i32]) {
        self.field_begin(LIST, id);
        self.list_begin(ELEM_I32, items.len());
        for v in items {
            self.write_i32_raw(*v);
        }
    }

    pub fn list_i64_field(&mut self, id: i16, items: &[i64]) {
        self.field_begin(LIST, id);
        self.list_begin(ELEM_I64, items.len());
        for v in items {
            self.write_i64_raw(*v);
        }
    }

    pub fn list_str_field(&mut self, id: i16, items: &[&str]) {
        self.field_begin(LIST, id);
        self.list_begin(ELEM_BINARY, items.len());
        for s in items {
            self.write_vlq(s.len() as u64);
            self.buf.extend_from_slice(s.as_bytes());
        }
    }

    pub fn list_bool_field(&mut self, id: i16, items: &[bool]) {
        self.field_begin(LIST, id);
        self.list_begin(ELEM_BOOL, items.len());
        for v in items {
            self.buf.push(if *v { 1 } else { 2 });
        }
    }

    fn list_begin(&mut self, elem: u8, len: usize) {
        if len < 15 {
            self.buf.push((len as u8) << 4 | elem);
        } else {
            self.buf.push(0xf0 | elem);
            self.write_vlq(len as u64);
        }
    }

    /// Empty struct used as a union arm (STRING, TYPE_ORDER, MilliSeconds, …).
    pub fn empty_struct_field(&mut self, id: i16) {
        self.struct_field(id, |_| {});
    }
}

// ---------------------------------------------------------------------------
// Page header
// ---------------------------------------------------------------------------

pub fn data_page_v1_header(
    uncompressed_page_size: i32,
    compressed_page_size: i32,
    num_values: i32,
) -> Vec<u8> {
    let mut c = Compact::new();
    // PageHeader
    c.field_i32(1, PAGE_DATA); // type
    c.field_i32(2, uncompressed_page_size);
    c.field_i32(3, compressed_page_size);
    // skip crc (4)
    c.struct_field(5, |c| {
        // DataPageHeader
        c.field_i32(1, num_values);
        c.field_i32(2, ENC_PLAIN);
        c.field_i32(3, ENC_RLE); // definition_level_encoding
        c.field_i32(4, ENC_RLE); // repetition_level_encoding
    });
    c.stop();
    c.into_inner()
}

// ---------------------------------------------------------------------------
// Compact decoder (page headers we emit)
// ---------------------------------------------------------------------------

/// Decoded DataPageHeader V1 plus the enclosing PageHeader size fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPageV1Header {
    pub page_type: i32,
    pub uncompressed_page_size: i32,
    pub compressed_page_size: i32,
    pub crc: Option<i32>,
    pub num_values: i32,
    pub encoding: i32,
    pub definition_level_encoding: i32,
    pub repetition_level_encoding: i32,
}

struct CompactReader<'a> {
    buf: &'a [u8],
    pos: usize,
    last: i16,
}

impl<'a> CompactReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            last: 0,
        }
    }

    fn read_u8(&mut self) -> Result<u8, String> {
        if self.pos >= self.buf.len() {
            return Err("unexpected end of thrift buffer".into());
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_vlq(&mut self) -> Result<u64, String> {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let b = self.read_u8()?;
            result |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift > 63 {
                return Err("varint too long".into());
            }
        }
    }

    fn read_zigzag(&mut self) -> Result<i64, String> {
        let n = self.read_vlq()?;
        Ok(((n >> 1) as i64) ^ -((n & 1) as i64))
    }

    fn next_field(&mut self) -> Result<Option<(u8, i16)>, String> {
        let header = self.read_u8()?;
        if header == STOP {
            return Ok(None);
        }
        let ty = header & 0x0f;
        let delta = (header >> 4) as i16;
        let id = if delta != 0 {
            self.last.wrapping_add(delta)
        } else {
            self.read_zigzag()? as i16
        };
        self.last = id;
        Ok(Some((ty, id)))
    }

    fn expect_i32(&mut self, ty: u8) -> Result<i32, String> {
        match ty {
            I16 | I32 | I64 => Ok(self.read_zigzag()? as i32),
            _ => Err(format!("expected integer type, got {ty}")),
        }
    }

    fn skip(&mut self, ty: u8) -> Result<(), String> {
        match ty {
            STOP => Ok(()),
            BOOL_TRUE | BOOL_FALSE => Ok(()),
            I16 | I32 | I64 => {
                self.read_zigzag()?;
                Ok(())
            }
            BINARY => {
                let n = self.read_vlq()? as usize;
                if self.pos + n > self.buf.len() {
                    return Err("binary overruns buffer".into());
                }
                self.pos += n;
                Ok(())
            }
            STRUCT => {
                let parent = self.last;
                self.last = 0;
                while let Some((inner, _)) = self.next_field()? {
                    self.skip(inner)?;
                }
                self.last = parent;
                Ok(())
            }
            LIST => {
                let header = self.read_u8()?;
                let elem = header & 0x0f;
                let mut len = (header >> 4) as usize;
                if len == 15 {
                    len = self.read_vlq()? as usize;
                }
                let parent = self.last;
                for _ in 0..len {
                    if elem == ELEM_STRUCT {
                        self.last = 0;
                        while let Some((inner, _)) = self.next_field()? {
                            self.skip(inner)?;
                        }
                    } else {
                        self.skip(elem)?;
                    }
                }
                self.last = parent;
                Ok(())
            }
            _ => Err(format!("cannot skip thrift type {ty}")),
        }
    }
}

/// Decode a DataPageHeader V1 (the only page header this crate emits).
/// Returns the header and the number of bytes consumed (header length).
pub fn decode_data_page_v1_header(bytes: &[u8]) -> Result<(DataPageV1Header, usize), String> {
    let mut r = CompactReader::new(bytes);
    let mut page_type = None;
    let mut uncompressed = None;
    let mut compressed = None;
    let mut crc = None;
    let mut num_values = None;
    let mut encoding = None;
    let mut def_enc = None;
    let mut rep_enc = None;
    let mut saw_data_header = false;

    while let Some((ty, id)) = r.next_field()? {
        match id {
            1 => page_type = Some(r.expect_i32(ty)?),
            2 => uncompressed = Some(r.expect_i32(ty)?),
            3 => compressed = Some(r.expect_i32(ty)?),
            4 => crc = Some(r.expect_i32(ty)?),
            5 => {
                if ty != STRUCT {
                    return Err(format!("data_page_header type {ty}, expected struct"));
                }
                let parent = r.last;
                r.last = 0;
                while let Some((ity, iid)) = r.next_field()? {
                    match iid {
                        1 => num_values = Some(r.expect_i32(ity)?),
                        2 => encoding = Some(r.expect_i32(ity)?),
                        3 => def_enc = Some(r.expect_i32(ity)?),
                        4 => rep_enc = Some(r.expect_i32(ity)?),
                        _ => r.skip(ity)?,
                    }
                }
                r.last = parent;
                saw_data_header = true;
            }
            _ => r.skip(ty)?,
        }
    }

    if !saw_data_header {
        return Err("missing DataPageHeader (field 5)".into());
    }
    Ok((
        DataPageV1Header {
            page_type: page_type.ok_or("missing PageHeader.type")?,
            uncompressed_page_size: uncompressed.ok_or("missing uncompressed_page_size")?,
            compressed_page_size: compressed.ok_or("missing compressed_page_size")?,
            crc,
            num_values: num_values.ok_or("missing DataPageHeader.num_values")?,
            encoding: encoding.ok_or("missing DataPageHeader.encoding")?,
            definition_level_encoding: def_enc
                .ok_or("missing DataPageHeader.definition_level_encoding")?,
            repetition_level_encoding: rep_enc
                .ok_or("missing DataPageHeader.repetition_level_encoding")?,
        },
        r.pos,
    ))
}

/// Decode an OffsetIndex we emit (`list<PageLocation>` in field 1).
pub fn decode_offset_index(bytes: &[u8]) -> Result<(Vec<PageLoc>, usize), String> {
    let mut r = CompactReader::new(bytes);
    let mut pages = Vec::new();
    while let Some((ty, id)) = r.next_field()? {
        if id != 1 {
            r.skip(ty)?;
            continue;
        }
        if ty != LIST {
            return Err(format!(
                "OffsetIndex.page_locations type {ty}, expected list"
            ));
        }
        let header = r.read_u8()?;
        let elem = header & 0x0f;
        if elem != ELEM_STRUCT {
            return Err(format!("OffsetIndex list elem {elem}, expected struct"));
        }
        let mut len = (header >> 4) as usize;
        if len == 15 {
            len = r.read_vlq()? as usize;
        }
        let parent = r.last;
        for _ in 0..len {
            r.last = 0;
            let mut offset = None;
            let mut compressed_page_size = None;
            let mut first_row_index = None;
            while let Some((ity, iid)) = r.next_field()? {
                match iid {
                    1 => offset = Some(r.read_zigzag()?),
                    2 => compressed_page_size = Some(r.expect_i32(ity)?),
                    3 => first_row_index = Some(r.read_zigzag()?),
                    _ => r.skip(ity)?,
                }
            }
            pages.push(PageLoc {
                offset: offset.ok_or("missing PageLocation.offset")?,
                compressed_page_size: compressed_page_size
                    .ok_or("missing PageLocation.compressed_page_size")?,
                first_row_index: first_row_index.ok_or("missing PageLocation.first_row_index")?,
            });
        }
        r.last = parent;
    }
    Ok((pages, r.pos))
}

// ---------------------------------------------------------------------------
// OffsetIndex
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct PageLoc {
    pub offset: i64,
    pub compressed_page_size: i32, // includes header
    pub first_row_index: i64,
}

pub fn offset_index(pages: &[PageLoc]) -> Vec<u8> {
    let mut c = Compact::new();
    c.list_struct_field(1, pages, |c, p| {
        c.field_i64(1, p.offset);
        c.field_i32(2, p.compressed_page_size);
        c.field_i64(3, p.first_row_index);
    });
    c.stop();
    c.into_inner()
}

// ---------------------------------------------------------------------------
// File metadata
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct SchemaField {
    pub name: String,
    pub physical: i32, // TYPE_*
    pub converted: Option<i32>,
    /// "utf8" | "ts_millis" | none
    pub logical: Option<&'static str>,
}

#[derive(Clone)]
pub struct ColumnChunkInfo {
    pub physical: i32,
    pub path: String,
    pub num_values: i64,
    pub uncompressed_size: i64, // including page headers
    pub compressed_size: i64,   // including page headers
    pub data_page_offset: i64,
    pub encodings: Vec<i32>,
    pub codec: i32,
    pub offset_index_offset: Option<i64>,
    pub offset_index_length: Option<i32>,
    /// PageEncodingStats.count for DATA_PAGE / PLAIN.
    pub data_page_count: i32,
}

#[derive(Clone)]
pub struct RowGroupInfo {
    pub columns: Vec<ColumnChunkInfo>,
    pub num_rows: i64,
    pub total_byte_size: i64, // uncompressed
    pub file_offset: i64,
    pub total_compressed_size: i64,
    pub ordinal: i16,
}

#[derive(Clone)]
pub struct Kv {
    pub key: String,
    pub value: String,
}

pub fn file_metadata(
    schema: &[SchemaField],
    num_rows: i64,
    row_groups: &[RowGroupInfo],
    created_by: &str,
    kv: &[Kv],
) -> Vec<u8> {
    let mut c = Compact::new();
    // FileMetaData
    c.field_i32(1, 1); // version

    // schema: root + fields
    // We encode as a list of SchemaElement. Root first.
    struct Elem {
        name: String,
        physical: Option<i32>,
        repetition: Option<i32>,
        num_children: Option<i32>,
        converted: Option<i32>,
        logical: Option<&'static str>,
    }
    let mut elems: Vec<Elem> = Vec::with_capacity(schema.len() + 1);
    elems.push(Elem {
        name: "schema".into(),
        physical: None,
        repetition: None,
        num_children: Some(schema.len() as i32),
        converted: None,
        logical: None,
    });
    for f in schema {
        elems.push(Elem {
            name: f.name.clone(),
            physical: Some(f.physical),
            repetition: Some(REP_REQUIRED),
            num_children: None,
            converted: f.converted,
            logical: f.logical,
        });
    }
    c.list_struct_field(2, &elems, |c, e| {
        if let Some(t) = e.physical {
            c.field_i32(1, t);
        }
        if let Some(r) = e.repetition {
            c.field_i32(3, r);
        }
        c.field_str(4, &e.name);
        if let Some(n) = e.num_children {
            c.field_i32(5, n);
        }
        if let Some(cv) = e.converted {
            c.field_i32(6, cv);
        }
        if let Some(log) = e.logical {
            c.struct_field(10, |c| write_logical(c, log));
        }
    });

    c.field_i64(3, num_rows);

    c.list_struct_field(4, row_groups, |c, rg| {
        c.list_struct_field(1, &rg.columns, |c, col| {
            // ColumnChunk
            c.field_i64(2, col.data_page_offset); // file_offset
            c.struct_field(3, |c| {
                // ColumnMetaData
                c.field_i32(1, col.physical);
                c.list_i32_field(2, &col.encodings);
                c.list_str_field(3, &[&col.path]);
                c.field_i32(4, col.codec);
                c.field_i64(5, col.num_values);
                c.field_i64(6, col.uncompressed_size);
                c.field_i64(7, col.compressed_size);
                c.field_i64(9, col.data_page_offset);
                // encoding_stats: DATA_PAGE / PLAIN × data_page_count
                c.list_struct_field(13, &[()], |c, _| {
                    c.field_i32(1, PAGE_DATA);
                    c.field_i32(2, ENC_PLAIN);
                    c.field_i32(3, col.data_page_count.max(1));
                });
            });
            if let Some(off) = col.offset_index_offset {
                c.field_i64(4, off);
            }
            if let Some(len) = col.offset_index_length {
                c.field_i32(5, len);
            }
        });
        c.field_i64(2, rg.total_byte_size);
        c.field_i64(3, rg.num_rows);
        c.field_i64(5, rg.file_offset);
        c.field_i64(6, rg.total_compressed_size);
        c.field_i16(7, rg.ordinal);
    });

    if !kv.is_empty() {
        c.list_struct_field(5, kv, |c, kv| {
            c.field_str(1, &kv.key);
            c.field_str(2, &kv.value);
        });
    }

    c.field_str(6, created_by);

    // column_orders: TYPE_ORDER for each leaf
    let orders: Vec<()> = schema.iter().map(|_| ()).collect();
    c.list_struct_field(7, &orders, |c, _| {
        // ColumnOrder union: field 1 = TYPE_ORDER (empty struct)
        c.empty_struct_field(1);
    });

    c.stop();
    c.into_inner()
}

fn write_logical(c: &mut Compact, kind: &str) {
    match kind {
        "utf8" => {
            // LogicalType.STRING = field 1, empty StringType
            c.empty_struct_field(1);
        }
        "ts_millis" => {
            // LogicalType.TIMESTAMP = field 8
            c.struct_field(8, |c| {
                // TimestampType
                c.field_bool(1, false); // isAdjustedToUTC
                c.struct_field(2, |c| {
                    // TimeUnit.MILLIS = field 1, empty MilliSeconds
                    c.empty_struct_field(1);
                });
            });
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_page_header_roundtrip(uncomp: i32, comp: i32, nvals: i32) {
        let bytes = data_page_v1_header(uncomp, comp, nvals);
        let (hdr, consumed) = decode_data_page_v1_header(&bytes).expect("decode");
        assert_eq!(
            consumed,
            bytes.len(),
            "decoder must consume the whole header"
        );
        assert_eq!(hdr.page_type, PAGE_DATA);
        assert_eq!(hdr.uncompressed_page_size, uncomp);
        assert_eq!(hdr.compressed_page_size, comp);
        assert_eq!(hdr.num_values, nvals);
        assert_eq!(hdr.encoding, ENC_PLAIN);
        assert_eq!(hdr.definition_level_encoding, ENC_RLE);
        assert_eq!(hdr.repetition_level_encoding, ENC_RLE);
        assert_eq!(hdr.crc, None);
    }

    #[test]
    fn page_header_nonempty() {
        let h = data_page_v1_header(100, 50, 10);
        assert!(h.len() > 8);
        assert_eq!(*h.last().unwrap(), 0); // struct stop
    }

    #[test]
    fn page_header_deterministic_roundtrip_bytes() {
        let a = data_page_v1_header(1000, 200, 42);
        let b = data_page_v1_header(1000, 200, 42);
        assert_eq!(a, b);
        assert!(a.len() > 4);
        // Different inputs → different encodings (size fields differ).
        let c = data_page_v1_header(1001, 200, 42);
        assert_ne!(a, c);
    }

    #[test]
    fn page_header_encode_decode_equal_fields() {
        // Tiny / typical / large / zigzag-interesting sizes used by the writers.
        for (uncomp, comp, nvals) in [
            (0, 0, 0),
            (100, 50, 10),
            (1000, 200, 42),
            (4096, 800, 80),
            (1_000_000, 200_000, 5_000),
            // Aligned pages pad with skippable frames → compressed ≫ uncompressed.
            (256, 12_288, 24),
            (64, 4096, 3),
            // Interleaved host page: concatenated zstd + skippable sibling frames.
            (48, 359, 4),
            (343, 343, 12),
        ] {
            assert_page_header_roundtrip(uncomp, comp, nvals);
        }
    }

    #[test]
    fn page_header_decode_rejects_truncated() {
        let bytes = data_page_v1_header(100, 50, 10);
        assert!(decode_data_page_v1_header(&bytes[..bytes.len() / 2]).is_err());
        assert!(decode_data_page_v1_header(&[]).is_err());
    }

    #[test]
    fn offset_index_encode_nonempty() {
        let pages = vec![
            PageLoc {
                offset: 4,
                compressed_page_size: 100,
                first_row_index: 0,
            },
            PageLoc {
                offset: 104,
                compressed_page_size: 80,
                first_row_index: 10,
            },
        ];
        let bytes = offset_index(&pages);
        assert!(bytes.len() > 8);
        assert_eq!(*bytes.last().unwrap(), 0);
    }

    #[test]
    fn offset_index_encode_decode_roundtrip() {
        let pages = vec![
            PageLoc {
                offset: 4,
                compressed_page_size: 100,
                first_row_index: 0,
            },
            PageLoc {
                offset: 104,
                compressed_page_size: 80,
                first_row_index: 10,
            },
            PageLoc {
                offset: 12_288,
                compressed_page_size: 4_200,
                first_row_index: 80,
            },
        ];
        let bytes = offset_index(&pages);
        let (decoded, consumed) = decode_offset_index(&bytes).expect("decode offset index");
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.len(), pages.len());
        for (a, b) in decoded.iter().zip(pages.iter()) {
            assert_eq!(a.offset, b.offset);
            assert_eq!(a.compressed_page_size, b.compressed_page_size);
            assert_eq!(a.first_row_index, b.first_row_index);
        }
    }
}

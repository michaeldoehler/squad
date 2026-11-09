//! Column chunk encodings.
//!
//! A chunk payload is
//!
//! ```text
//! has_nulls:u8  [validity bitmap, (rows+7)/8 bytes]  body
//! ```
//!
//! and the body depends on the column type:
//!
//! * integers — `tag:u8` then
//!   `0` plain (8 bytes each),
//!   `1` frame of reference: `min:i64 width:u8` + bit-packed `value - min`,
//!   `2` delta: `first:i64 min_delta:i64 width:u8` + bit-packed
//!   `delta - min_delta` for the remaining values,
//!   `3` run length: `runs:u32` then `value:i64 count:u32` per run;
//! * booleans — a bitmap;
//! * floats — 8 bytes each;
//! * decimals — `tag:u8`: `0` 16 bytes each, `1` all values fit 64 bits and
//!   are stored with the integer encodings;
//! * bytes/text — `tag:u8`: `0` plain: lengths (integer-encoded),
//!   `data_len:u32`, data; `1` dictionary: `entries:u32`, the dictionary in
//!   plain form, `width:u8`, bit-packed codes.
//!
//! The writer picks the smallest encoding for each chunk.

use std::sync::Arc;

use crate::types::{BytesColumn, BytesData, Column, ColumnData, ColumnType};
use crate::util::{bits_for, corrupt, pack, packed_len, unpack, Cur, FastMap, Put};
use crate::Result;

fn put_bitmap(out: &mut Vec<u8>, bits: &[bool]) {
    for chunk in bits.chunks(8) {
        let mut b = 0u8;
        for (i, &v) in chunk.iter().enumerate() {
            b |= (v as u8) << i;
        }
        out.push(b);
    }
}

fn get_bitmap(cur: &mut Cur, n: usize) -> Result<Vec<bool>> {
    let bytes = cur.take(n.div_ceil(8))?;
    Ok((0..n).map(|i| bytes[i / 8] >> (i % 8) & 1 == 1).collect())
}

pub(crate) fn encode_ints(v: &[i64], out: &mut Vec<u8>) {
    let n = v.len();
    if n == 0 {
        out.push(0);
        return;
    }
    let min = *v.iter().min().unwrap();
    let for_width = bits_for(v.iter().map(|x| x.wrapping_sub(min) as u64).max().unwrap());
    let mut runs = 1usize;
    let mut min_delta = i64::MAX;
    for w in v.windows(2) {
        runs += (w[0] != w[1]) as usize;
        min_delta = min_delta.min(w[1].wrapping_sub(w[0]));
    }
    let delta_width = if n > 1 {
        bits_for(v.windows(2).map(|w| w[1].wrapping_sub(w[0]).wrapping_sub(min_delta) as u64).max().unwrap())
    } else {
        0
    };
    let plain = 8 * n;
    let frame = 9 + packed_len(n, for_width);
    let delta = 17 + packed_len(n - 1, delta_width);
    let rle = 4 + 12 * runs;
    let best = plain.min(frame).min(delta).min(rle);
    if best == rle {
        out.push(3);
        out.put_u32(runs as u32);
        let mut i = 0;
        while i < n {
            let mut j = i + 1;
            while j < n && v[j] == v[i] {
                j += 1;
            }
            out.put_u64(v[i] as u64);
            out.put_u32((j - i) as u32);
            i = j;
        }
    } else if best == frame {
        out.push(1);
        out.put_u64(min as u64);
        out.push(for_width);
        pack(v.iter().map(|x| x.wrapping_sub(min) as u64), for_width, out);
    } else if best == delta {
        out.push(2);
        out.put_u64(v[0] as u64);
        out.put_u64(min_delta as u64);
        out.push(delta_width);
        pack(v.windows(2).map(|w| w[1].wrapping_sub(w[0]).wrapping_sub(min_delta) as u64), delta_width, out);
    } else {
        out.push(0);
        for x in v {
            out.put_u64(*x as u64);
        }
    }
}

pub(crate) fn decode_ints(cur: &mut Cur, n: usize) -> Result<Vec<i64>> {
    match cur.u8()? {
        0 => {
            let bytes = cur.take(n.checked_mul(8).ok_or_else(|| corrupt("row count overflow"))?)?;
            Ok(bytes.chunks_exact(8).map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect())
        }
        1 => {
            let min = cur.i64()?;
            let width = cur.u8()?;
            let packed = unpack(cur, width, n)?;
            Ok(packed.into_iter().map(|p| min.wrapping_add(p as i64)).collect())
        }
        2 => {
            if n == 0 {
                return Err(corrupt("delta encoding of an empty chunk"));
            }
            let first = cur.i64()?;
            let min_delta = cur.i64()?;
            let width = cur.u8()?;
            let packed = unpack(cur, width, n - 1)?;
            let mut out = Vec::with_capacity(n);
            let mut cur_v = first;
            out.push(cur_v);
            for p in packed {
                cur_v = cur_v.wrapping_add(min_delta.wrapping_add(p as i64));
                out.push(cur_v);
            }
            Ok(out)
        }
        3 => {
            let runs = cur.u32()? as usize;
            let mut out = Vec::with_capacity(n);
            for _ in 0..runs {
                let v = cur.i64()?;
                let count = cur.u32()? as usize;
                if out.len() + count > n {
                    return Err(corrupt("run-length data exceeds the row count"));
                }
                out.resize(out.len() + count, v);
            }
            if out.len() != n {
                return Err(corrupt("run-length data does not cover the chunk"));
            }
            Ok(out)
        }
        _ => Err(corrupt("unknown integer encoding")),
    }
}

fn encode_plain_bytes(d: &BytesData, n: usize, get: impl Fn(usize) -> usize, out: &mut Vec<u8>) {
    let _ = d;
    let lens: Vec<i64> = (0..n).map(|i| get(i) as i64).collect();
    encode_ints(&lens, out);
}

fn put_bytes_data(values: &BytesData, out: &mut Vec<u8>) {
    encode_plain_bytes(values, values.len(), |i| values.get(i).len(), out);
    out.put_bytes(&values.data[values.offsets[0] as usize..*values.offsets.last().unwrap() as usize]);
}

fn get_bytes_data(cur: &mut Cur, n: usize) -> Result<BytesData> {
    let lens = decode_ints(cur, n)?;
    let data = cur.bytes()?;
    let mut offsets = Vec::with_capacity(n + 1);
    let mut pos = 0u64;
    offsets.push(0);
    for l in lens {
        if l < 0 {
            return Err(corrupt("negative value length"));
        }
        pos += l as u64;
        if pos > data.len() as u64 {
            return Err(corrupt("value lengths exceed the data"));
        }
        offsets.push(pos as u32);
    }
    if pos != data.len() as u64 {
        return Err(corrupt("value lengths do not cover the data"));
    }
    Ok(BytesData { offsets, data: data.to_vec() })
}

fn encode_bytes(col: &BytesColumn, out: &mut Vec<u8>) {
    let n = col.len();
    // Try a dictionary: worthwhile when values repeat.
    let limit = (n / 2 + 16).min(1 << 20);
    let mut ids: FastMap<&[u8], u32> = FastMap::default();
    let mut codes = Vec::with_capacity(n);
    let mut dict = BytesData::new();
    let mut ok = true;
    match col {
        BytesColumn::Dict { codes: c, dict: d } if d.len() <= limit => {
            codes = c.clone();
            dict = (**d).clone();
        }
        _ => {
            for i in 0..n {
                let v = col.get(i);
                let next = ids.len() as u32;
                let id = *ids.entry(v).or_insert_with(|| {
                    dict.push(v);
                    next
                });
                if ids.len() > limit {
                    ok = false;
                    break;
                }
                codes.push(id);
            }
        }
    }
    if ok && n > 0 {
        out.push(1);
        out.put_u32(dict.len() as u32);
        put_bytes_data(&dict, out);
        let width = bits_for(dict.len().saturating_sub(1) as u64);
        out.push(width);
        pack(codes.iter().map(|c| *c as u64), width, out);
    } else {
        out.push(0);
        let mut plain = BytesData::new();
        (0..n).for_each(|i| plain.push(col.get(i)));
        put_bytes_data(&plain, out);
    }
}

fn decode_bytes(cur: &mut Cur, n: usize) -> Result<BytesColumn> {
    match cur.u8()? {
        0 => Ok(BytesColumn::Plain(get_bytes_data(cur, n)?)),
        1 => {
            let entries = cur.u32()? as usize;
            if entries > cur.remaining() {
                return Err(corrupt("dictionary larger than the chunk"));
            }
            let dict = get_bytes_data(cur, entries)?;
            let width = cur.u8()?;
            let codes: Vec<u32> = unpack(cur, width, n)?.into_iter().map(|c| c as u32).collect();
            if codes.iter().any(|c| *c as usize >= entries) {
                return Err(corrupt("dictionary code out of range"));
            }
            Ok(BytesColumn::Dict { codes, dict: Arc::new(dict) })
        }
        _ => Err(corrupt("unknown bytes encoding")),
    }
}

/// Encodes a column chunk (without framing, compression or checksum).
pub(crate) fn encode_column(col: &Column) -> Vec<u8> {
    let mut out = Vec::new();
    match &col.validity {
        Some(v) => {
            out.push(1);
            put_bitmap(&mut out, v);
        }
        None => out.push(0),
    }
    match &col.data {
        ColumnData::Bool(v) => put_bitmap(&mut out, v),
        ColumnData::Int64(v) => encode_ints(v, &mut out),
        ColumnData::Float64(v) => v.iter().for_each(|f| out.put_u64(f.to_bits())),
        ColumnData::Decimal(v) => {
            if v.iter().all(|d| i64::try_from(*d).is_ok()) {
                out.push(1);
                let narrow: Vec<i64> = v.iter().map(|d| *d as i64).collect();
                encode_ints(&narrow, &mut out);
            } else {
                out.push(0);
                v.iter().for_each(|d| out.extend_from_slice(&d.to_le_bytes()));
            }
        }
        ColumnData::Bytes(b) => encode_bytes(b, &mut out),
    }
    out
}

/// Decodes a chunk of `rows` values of type `ty`.
pub(crate) fn decode_column(payload: &[u8], ty: ColumnType, rows: usize) -> Result<Column> {
    let mut cur = Cur::new(payload);
    let validity = match cur.u8()? {
        0 => None,
        1 => Some(get_bitmap(&mut cur, rows)?),
        _ => return Err(corrupt("invalid validity marker")),
    };
    let data = match ty {
        ColumnType::Bool => ColumnData::Bool(get_bitmap(&mut cur, rows)?),
        ColumnType::Int64 => ColumnData::Int64(decode_ints(&mut cur, rows)?),
        ColumnType::Float64 => {
            let bytes = cur.take(rows.checked_mul(8).ok_or_else(|| corrupt("row count overflow"))?)?;
            ColumnData::Float64(
                bytes.chunks_exact(8).map(|c| f64::from_bits(u64::from_le_bytes(c.try_into().unwrap()))).collect(),
            )
        }
        ColumnType::Decimal { .. } => match cur.u8()? {
            0 => {
                let bytes = cur.take(rows.checked_mul(16).ok_or_else(|| corrupt("row count overflow"))?)?;
                ColumnData::Decimal(
                    bytes.chunks_exact(16).map(|c| i128::from_le_bytes(c.try_into().unwrap())).collect(),
                )
            }
            1 => ColumnData::Decimal(decode_ints(&mut cur, rows)?.into_iter().map(i128::from).collect()),
            _ => return Err(corrupt("unknown decimal encoding")),
        },
        ColumnType::Bytes | ColumnType::Utf8 => ColumnData::Bytes(decode_bytes(&mut cur, rows)?),
    };
    if cur.remaining() != 0 {
        return Err(corrupt("trailing bytes in column chunk"));
    }
    Ok(Column { data, validity })
}

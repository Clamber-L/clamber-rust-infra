//! `by_execute_at` 索引的 key 编解码。
//!
//! ```text
//! key = execute_at(8 字节, big-endian) || message_id(UTF-8)
//! ```
//!
//! 大端**不是实现细节**。RocksDB 按字节序比较 key，而我们需要
//! "字节序 = 时间序"，否则 range scan 拿不到按到期顺序排列的结果，
//! §4.2 的预加载与 §17 的 recovery 都会失效。

/// `execute_at` 在索引 key 中占用的字节数。
pub const TS_LEN: usize = 8;

/// 构造索引 key。
///
/// `execute_at` 可能为负(1970 年之前)，而 `i64` 的补码表示下负数的
/// 最高位为 1，直接按无符号字节序比较会把负数排到正数之后。异或符号位
/// 把 `i64` 的全序映射到 `u64` 的字节序 —— 这是编码有符号整数为可排序
/// 字节串的标准做法。
pub fn index_key(execute_at: i64, message_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(TS_LEN + message_id.len());
    key.extend_from_slice(&encode_ts(execute_at));
    key.extend_from_slice(message_id.as_bytes());
    key
}

/// `execute_at` 的可排序 8 字节编码。
pub fn encode_ts(execute_at: i64) -> [u8; TS_LEN] {
    ((execute_at as u64) ^ (1u64 << 63)).to_be_bytes()
}

/// 解析索引 key，返回 `(execute_at, message_id)`。
pub fn decode_index_key(key: &[u8]) -> Option<(i64, &str)> {
    if key.len() < TS_LEN {
        return None;
    }
    let (ts, rest) = key.split_at(TS_LEN);
    let raw = u64::from_be_bytes(ts.try_into().ok()?);
    let execute_at = (raw ^ (1u64 << 63)) as i64;
    std::str::from_utf8(rest).ok().map(|id| (execute_at, id))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 核心性质：key 的字节序必须等于 execute_at 的数值序。
    #[test]
    fn byte_order_matches_time_order() {
        let mut timestamps = [
            i64::MIN,
            -1_000_000,
            -1,
            0,
            1,
            1_000,
            1_700_000_000_000,
            i64::MAX,
        ];

        let mut keys: Vec<Vec<u8>> = timestamps.iter().map(|&t| index_key(t, "m")).collect();
        keys.sort();
        timestamps.sort();

        let expected: Vec<Vec<u8>> = timestamps.iter().map(|&t| index_key(t, "m")).collect();
        assert_eq!(keys, expected, "byte order diverged from time order");
    }

    /// 小端编码会在这里失败 —— 保留此测试作为回归。
    #[test]
    fn little_endian_would_break_ordering() {
        let early = 1_000i64;
        let late = 2_000i64;

        assert!(
            index_key(early, "m") < index_key(late, "m"),
            "big-endian ordering broken"
        );
        // 对照：小端下 1000 的首字节 0xe8 > 2000 的首字节 0xd0
        assert!(early.to_le_bytes() > late.to_le_bytes());
    }

    #[test]
    fn roundtrips() {
        for ts in [i64::MIN, -1, 0, 1_700_000_000_000, i64::MAX] {
            let key = index_key(ts, "1734567890123456789");
            let (got_ts, got_id) = decode_index_key(&key).expect("decode");
            assert_eq!(got_ts, ts);
            assert_eq!(got_id, "1734567890123456789");
        }
    }

    /// 同一毫秒内多条消息按 message_id 排序，保证 key 唯一且顺序确定。
    #[test]
    fn same_timestamp_orders_by_message_id() {
        let a = index_key(1_000, "aaa");
        let b = index_key(1_000, "bbb");
        assert!(a < b);
    }

    #[test]
    fn rejects_truncated_key() {
        assert!(decode_index_key(&[0u8; 4]).is_none());
    }

    /// seek 前缀必须与完整 key 的时间戳部分一致，否则定位不到。
    #[test]
    fn encode_ts_is_the_key_prefix() {
        let key = index_key(1_700_000_000_000, "m1");
        assert_eq!(&key[..TS_LEN], &encode_ts(1_700_000_000_000));
    }
}

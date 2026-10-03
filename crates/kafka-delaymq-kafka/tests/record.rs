//! 线格式的编解码与校验。不需要 broker。

use kafka_delaymq_core::InvalidMessage;
use kafka_delaymq_kafka::DelayRecord;

const SNOWFLAKE: &str = "1734567890123456789";

fn record() -> DelayRecord {
    DelayRecord {
        message_id: SNOWFLAKE.to_string(),
        target_topic: "business-topic".to_string(),
        execute_at: 1_700_000_000_000,
        payload: b"hello".to_vec(),
        headers: vec![("trace".into(), "abc".into())],
    }
}

#[test]
fn record_roundtrips_through_bincode() {
    let original = record();
    let bytes = bincode::serialize(&original).unwrap();
    let decoded: DelayRecord = bincode::deserialize(&bytes).unwrap();

    assert_eq!(decoded, original);
}

/// 来源坐标由消费端填入，不在线格式里 —— 业务方不知道自己落在哪个
/// partition/offset。
#[test]
fn into_message_attaches_source_coordinates() {
    let message = record().into_message(3, 12_345).unwrap();

    assert_eq!(message.message_id, SNOWFLAKE);
    assert_eq!(message.target_topic, "business-topic");
    assert_eq!(message.source_partition, 3);
    assert_eq!(message.source_offset, 12_345);
    assert_eq!(message.payload, b"hello");
    assert_eq!(message.headers, vec![("trace".to_string(), "abc".to_string())]);
}

/// 转换时顺带校验 —— 非法消息不该进入系统。
#[test]
fn into_message_rejects_bad_snowflake_id() {
    let mut r = record();
    r.message_id = "not-a-snowflake".into();

    assert!(matches!(
        r.into_message(0, 0),
        Err(InvalidMessage::MalformedMessageId { .. })
    ));
}

#[test]
fn into_message_rejects_empty_id() {
    let mut r = record();
    r.message_id = String::new();

    assert_eq!(r.into_message(0, 0), Err(InvalidMessage::EmptyMessageId));
}

#[test]
fn into_message_rejects_empty_target_topic() {
    let mut r = record();
    r.target_topic = String::new();

    assert!(matches!(
        r.into_message(0, 0),
        Err(InvalidMessage::EmptyTargetTopic { .. })
    ));
}

#[test]
fn empty_payload_survives_roundtrip() {
    let mut r = record();
    r.payload = Vec::new();

    let bytes = bincode::serialize(&r).unwrap();
    let decoded: DelayRecord = bincode::deserialize(&bytes).unwrap();

    assert!(decoded.payload.is_empty());
    assert!(decoded.into_message(0, 0).is_ok(), "empty payload is valid");
}

#[test]
fn large_payload_survives_roundtrip() {
    let mut r = record();
    r.payload = vec![0x5a; 512 * 1024];

    let bytes = bincode::serialize(&r).unwrap();
    let decoded: DelayRecord = bincode::deserialize(&bytes).unwrap();

    assert_eq!(decoded.payload.len(), 512 * 1024);
    assert!(decoded.payload.iter().all(|&b| b == 0x5a));
}

#[test]
fn headers_are_optional_on_the_wire() {
    let mut r = record();
    r.headers = Vec::new();

    let bytes = bincode::serialize(&r).unwrap();
    let decoded: DelayRecord = bincode::deserialize(&bytes).unwrap();

    assert!(decoded.headers.is_empty());
}

#[test]
fn garbage_bytes_fail_to_decode() {
    let result: Result<DelayRecord, _> = bincode::deserialize(b"not a valid record at all");
    assert!(result.is_err());
}

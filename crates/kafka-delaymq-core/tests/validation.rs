//! 入口校验。`message_id` 由业务方生成，合法性必须在入口处挡住，
//! 否则会一路带到存储层才暴露。见架构文档 §15.1。

use kafka_delaymq_core::{DelayMessage, InvalidMessage};

fn message(id: &str) -> DelayMessage {
    DelayMessage {
        message_id: id.to_string(),
        target_topic: "business-topic".to_string(),
        source_partition: 0,
        source_offset: 0,
        execute_at: 1_700_000_000_000,
        payload: b"body".to_vec(),
        headers: Vec::new(),
    }
}

#[test]
fn valid_snowflake_id_passes() {
    // 典型的雪花 ID：19 位十进制
    assert!(message("1734567890123456789").validate().is_ok());
}

#[test]
fn empty_message_id_is_rejected() {
    assert_eq!(
        message("").validate(),
        Err(InvalidMessage::EmptyMessageId)
    );
}

#[test]
fn non_numeric_message_id_is_rejected() {
    assert!(matches!(
        message("not-a-snowflake").validate(),
        Err(InvalidMessage::MalformedMessageId { .. })
    ));
}

/// 雪花 ID 恒为正 —— 符号位不用。负数或零说明上游生成器有问题。
#[test]
fn non_positive_message_id_is_rejected() {
    for id in ["0", "-1", "-1734567890123456789"] {
        assert!(
            matches!(
                message(id).validate(),
                Err(InvalidMessage::MalformedMessageId { .. })
            ),
            "id {id:?} should be rejected"
        );
    }
}

/// 超过 i64 范围说明不是雪花 ID。
#[test]
fn overflowing_message_id_is_rejected() {
    assert!(matches!(
        message("99999999999999999999999").validate(),
        Err(InvalidMessage::MalformedMessageId { .. })
    ));
}

#[test]
fn empty_target_topic_is_rejected() {
    let mut msg = message("1734567890123456789");
    msg.target_topic = String::new();

    assert!(matches!(
        msg.validate(),
        Err(InvalidMessage::EmptyTargetTopic { .. })
    ));
}

/// 空 payload 是合法的 —— 业务可能只靠 headers 传递信息。
#[test]
fn empty_payload_is_allowed() {
    let mut msg = message("1734567890123456789");
    msg.payload = Vec::new();

    assert!(msg.validate().is_ok());
}

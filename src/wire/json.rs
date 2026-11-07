use crate::error::{Result, RpcError};
use prost::Message;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use serde::Serialize;
use serde_json::Value;
use std::{collections::BTreeMap, sync::LazyLock};

static MESSAGES: LazyLock<Result<BTreeMap<String, Vec<FieldDescriptorProto>>>> =
    LazyLock::new(|| {
        let descriptors = FileDescriptorSet::decode(super::DESCRIPTORS)?;
        let mut messages = BTreeMap::new();
        for file in descriptors.file {
            let prefix = format!(
                ".{}",
                file.package
                    .ok_or_else(|| RpcError::corrupt("descriptor package missing"))?
            );
            for message in file.message_type {
                collect(&prefix, message, &mut messages)?;
            }
        }
        Ok(messages)
    });

fn collect(
    prefix: &str,
    message: DescriptorProto,
    output: &mut BTreeMap<String, Vec<FieldDescriptorProto>>,
) -> Result<()> {
    let name = format!(
        "{prefix}.{}",
        message
            .name
            .ok_or_else(|| RpcError::corrupt("descriptor message name missing"))?
    );
    output.insert(name.clone(), message.field);
    for nested in message.nested_type {
        collect(&name, nested, output)?;
    }
    Ok(())
}

pub fn protobuf_json<T: Serialize + Message>(message: &T) -> Result<Value> {
    let messages = MESSAGES.as_ref().map_err(Clone::clone)?;
    let rust_name = std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .ok_or_else(|| RpcError::corrupt("protobuf type name missing"))?;
    let candidates: Vec<_> = messages
        .keys()
        .filter(|name| name.rsplit('.').next() == Some(rust_name))
        .collect();
    if candidates.len() != 1 {
        return Err(RpcError::corrupt(
            "protobuf JSON type is missing or ambiguous",
        ));
    }
    rewrite(messages, candidates[0], serde_json::to_value(message)?)
}

fn rewrite(
    messages: &BTreeMap<String, Vec<FieldDescriptorProto>>,
    name: &str,
    value: Value,
) -> Result<Value> {
    let fields = messages
        .get(name)
        .ok_or_else(|| RpcError::corrupt("protobuf JSON descriptor missing"))?;
    match value {
        Value::Object(values) => {
            let mut result = serde_json::Map::new();
            for (key, value) in values {
                let field = fields
                    .iter()
                    .find(|field| {
                        field.json_name.as_deref() == Some(&key)
                            || field.name.as_deref() == Some(&key)
                    })
                    .ok_or_else(|| {
                        RpcError::corrupt("serialized protobuf field has no descriptor")
                    })?;
                let value = if field.r#type
                    == Some(prost_types::field_descriptor_proto::Type::Message as i32)
                {
                    let message_type = field
                        .type_name
                        .as_deref()
                        .ok_or_else(|| RpcError::corrupt("protobuf field type missing"))?;
                    match value {
                        Value::Array(values) => Value::Array(
                            values
                                .into_iter()
                                .map(|value| rewrite(messages, message_type, value))
                                .collect::<Result<Vec<_>>>()?,
                        ),
                        Value::Null => Value::Null,
                        value => rewrite(messages, message_type, value)?,
                    }
                } else {
                    value
                };
                result.insert(
                    field
                        .name
                        .clone()
                        .ok_or_else(|| RpcError::corrupt("protobuf field name missing"))?,
                    value,
                );
            }
            Ok(Value::Object(result))
        }
        value => Ok(value),
    }
}

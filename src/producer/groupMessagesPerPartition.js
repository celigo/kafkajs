const getUniformExplicitPartition = messages => {
  if (messages.length === 0) {
    return null
  }

  const { partitionNumber } = messages[0]
  if (
    partitionNumber !== undefined &&
    partitionNumber !== null &&
    messages.every(message => message.partitionNumber === partitionNumber)
  ) {
    return partitionNumber
  }

  const { partition } = messages[0]
  if (
    partition !== undefined &&
    partition !== null &&
    messages.every(message => message.partition === partition)
  ) {
    return partition
  }

  return null
}

module.exports = ({ topic, partitionMetadata, messages, partitioner }) => {
  if (partitionMetadata.length === 0) {
    return {}
  }

  if (messages.length === 0) {
    return {}
  }

  const uniformPartition = getUniformExplicitPartition(messages)
  if (uniformPartition !== null) {
    // A uniform batch still consults the partitioner (once, with a
    // representative message): explicit partition fields are hints, and the
    // partitioner owns the final routing decision — e.g. single-partition
    // shortcuts, allow-listed topics, or clamping to existing partitions.
    // Returning the raw hint here would silently produce to partitions that
    // may not exist on the topic.
    const partition = partitioner({ topic, partitionMetadata, message: messages[0] })
    return { [partition]: messages }
  }

  if (messages.length === 1) {
    const partition = partitioner({ topic, partitionMetadata, message: messages[0] })
    return { [partition]: messages }
  }

  const result = {}
  for (const message of messages) {
    const partition = partitioner({ topic, partitionMetadata, message })
    let bucket = result[partition]
    if (!bucket) {
      bucket = []
      result[partition] = bucket
    }
    bucket.push(message)
  }
  return result
}

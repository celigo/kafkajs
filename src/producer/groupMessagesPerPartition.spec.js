const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const { createModPartitioner } = require('testHelpers')

describe('Producer > groupMessagesPerPartition', () => {
  let topic, partitionMetadata, messages, partitioner

  beforeEach(() => {
    topic = 'test-topic'
    partitionMetadata = [
      { partitionId: 1, leader: 1 },
      { partitionId: 2, leader: 2 },
      { partitionId: 0, leader: 0 },
    ]

    messages = [
      { key: '1' },
      { key: '2' },
      { key: '3' },
      { key: '4' },
      { key: '5' },
      { key: '6' },
      { key: '7' },
      { key: '8' },
      { key: '9' },
    ]
    partitioner = createModPartitioner()
  })

  test('group messages per partition', () => {
    const result = groupMessagesPerPartition({ topic, partitionMetadata, messages, partitioner })
    expect(result).toEqual({
      '0': [{ key: '3' }, { key: '6' }, { key: '9' }],
      '1': [{ key: '1' }, { key: '4' }, { key: '7' }],
      '2': [{ key: '2' }, { key: '5' }, { key: '8' }],
    })
  })

  test('returns empty when called with no partition metadata', () => {
    expect(
      groupMessagesPerPartition({ topic, partitionMetadata: [], messages, partitioner })
    ).toEqual({})
  })

  test('returns empty when called with no messages', () => {
    expect(
      groupMessagesPerPartition({ topic, partitionMetadata, messages: [], partitioner })
    ).toEqual({})
  })

  test('reuses the messages array and calls the partitioner once when every message has the same partitionNumber', () => {
    const flowEventMessages = [
      { value: 'a', partitionNumber: 2 },
      { value: 'b', partitionNumber: 2 },
      { value: 'c', partitionNumber: 2 },
    ]
    const explicitPartitioner = jest.fn(({ message }) => message.partitionNumber)

    const result = groupMessagesPerPartition({
      topic,
      partitionMetadata,
      messages: flowEventMessages,
      partitioner: explicitPartitioner,
    })

    expect(result).toEqual({ 2: flowEventMessages })
    expect(result[2]).toBe(flowEventMessages)
    expect(explicitPartitioner).toHaveBeenCalledTimes(1)
    expect(explicitPartitioner).toHaveBeenCalledWith({
      topic,
      partitionMetadata,
      message: flowEventMessages[0],
    })
  })

  test('reuses the messages array and calls the partitioner once when every message has the same partition', () => {
    const explicitPartitionMessages = [
      { key: 'a', partition: 1 },
      { key: 'b', partition: 1 },
    ]
    const explicitPartitioner = jest.fn(({ message }) => message.partition)

    const result = groupMessagesPerPartition({
      topic,
      partitionMetadata,
      messages: explicitPartitionMessages,
      partitioner: explicitPartitioner,
    })

    expect(result).toEqual({ 1: explicitPartitionMessages })
    expect(result[1]).toBe(explicitPartitionMessages)
    expect(explicitPartitioner).toHaveBeenCalledTimes(1)
  })

  test('uniform batches do not bypass a partitioner that redirects out-of-range partition hints', () => {
    // Regression: a topic with a single physical partition receiving a batch
    // whose partitionNumber points past it. Custom partitioners (e.g. a
    // single-partition shortcut) must stay authoritative — using the raw hint
    // would produce to a partition that does not exist.
    const singlePartitionMetadata = [{ partitionId: 0, leader: 0 }]
    const outOfRangeMessages = [
      { value: 'a', partitionNumber: 7 },
      { value: 'b', partitionNumber: 7 },
    ]
    const singlePartitionShortcutPartitioner = jest.fn(({ partitionMetadata: metadata, message }) =>
      metadata.length === 1 ? metadata[0].partitionId : message.partitionNumber
    )

    const result = groupMessagesPerPartition({
      topic,
      partitionMetadata: singlePartitionMetadata,
      messages: outOfRangeMessages,
      partitioner: singlePartitionShortcutPartitioner,
    })

    expect(result).toEqual({ 0: outOfRangeMessages })
    expect(result[7]).toBeUndefined()
    expect(singlePartitionShortcutPartitioner).toHaveBeenCalledTimes(1)
  })

  test('groups a single message without copying the array', () => {
    const singleMessage = [{ key: 'only' }]

    const result = groupMessagesPerPartition({
      topic,
      partitionMetadata,
      messages: singleMessage,
      partitioner,
    })

    expect(result).toEqual({ 0: singleMessage })
    expect(result[0]).toBe(singleMessage)
  })
})

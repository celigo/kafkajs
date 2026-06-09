const CooperativeStickyAssigner = require('./index')
const { MemberAssignment, MemberMetadata } = require('../../assignerProtocol')

function encodePriorAssignment(version, assignment, topics) {
  const userData = MemberAssignment.encode({ version, assignment })
  return MemberMetadata.encode({ version, topics, userData })
}

describe('Consumer > assigners > CooperativeStickyAssigner', () => {
  let cluster, topics, metadata, assigner

  beforeEach(() => {
    metadata = {}
    cluster = { findTopicPartitionMetadata: topic => metadata[topic] }
    assigner = CooperativeStickyAssigner({ cluster })
    topics = ['topic-A', 'topic-B']
  })

  describe('#assign', () => {
    test('assign all topic-partitions evenly with no prior assignment', async () => {
      metadata['topic-A'] = Array(6)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      metadata['topic-B'] = Array(3)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      const members = [{ memberId: 'member-2' }, { memberId: 'member-1' }, { memberId: 'member-3' }]

      const assignment = await assigner.assign({ members, topics })
      const decoded = assignment.reduce((acc, { memberId, memberAssignment }) => {
        acc[memberId] = MemberAssignment.decode(memberAssignment).assignment
        return acc
      }, {})

      const totalPartitions = Object.values(decoded).reduce((sum, a) => {
        return sum + Object.values(a).reduce((s, p) => s + p.length, 0)
      }, 0)

      expect(totalPartitions).toBe(9)

      Object.values(decoded).forEach(memberAssignment => {
        const count = Object.values(memberAssignment).reduce((s, p) => s + p.length, 0)
        expect(count).toBeGreaterThanOrEqual(3)
        expect(count).toBeLessThanOrEqual(3)
      })
    })

    test('retain prior assignments and only redistribute what needs to move', async () => {
      metadata['topic-A'] = Array(6)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      const members = [
        {
          memberId: 'member-1',
          memberMetadata: encodePriorAssignment(1, { 'topic-A': [0, 1, 2] }, ['topic-A']),
        },
        {
          memberId: 'member-2',
          memberMetadata: encodePriorAssignment(1, { 'topic-A': [3, 4, 5] }, ['topic-A']),
        },
        { memberId: 'member-3' },
      ]

      const assignment = await assigner.assign({ members, topics: ['topic-A'] })
      const decoded = assignment.reduce((acc, { memberId, memberAssignment }) => {
        acc[memberId] = MemberAssignment.decode(memberAssignment).assignment
        return acc
      }, {})

      const member1Partitions = decoded['member-1']['topic-A'] || []
      const member2Partitions = decoded['member-2']['topic-A'] || []
      const member3Partitions = decoded['member-3']['topic-A'] || []

      expect(member1Partitions.length + member2Partitions.length + member3Partitions.length).toBe(6)

      // Each member should have exactly 2 partitions (6 / 3)
      expect(member1Partitions.length).toBe(2)
      expect(member2Partitions.length).toBe(2)
      expect(member3Partitions.length).toBe(2)

      // member-1's partitions should be a subset of their prior assignment [0,1,2]
      member1Partitions.forEach(p => {
        expect([0, 1, 2]).toContain(p)
      })

      // member-2's partitions should be a subset of their prior assignment [3,4,5]
      member2Partitions.forEach(p => {
        expect([3, 4, 5]).toContain(p)
      })
    })

    test('handle member leaving gracefully', async () => {
      metadata['topic-A'] = Array(4)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      const members = [
        {
          memberId: 'member-1',
          memberMetadata: encodePriorAssignment(1, { 'topic-A': [0, 1] }, ['topic-A']),
        },
      ]

      const assignment = await assigner.assign({ members, topics: ['topic-A'] })
      const decoded = assignment.reduce((acc, { memberId, memberAssignment }) => {
        acc[memberId] = MemberAssignment.decode(memberAssignment).assignment
        return acc
      }, {})

      expect(decoded['member-1']['topic-A'].sort()).toEqual([0, 1, 2, 3])
    })

    test('handle member with no prior metadata', async () => {
      metadata['topic-A'] = [{ partitionId: 0 }, { partitionId: 1 }]

      const members = [
        { memberId: 'member-1', memberMetadata: Buffer.from('invalid') },
        { memberId: 'member-2' },
      ]

      const assignment = await assigner.assign({ members, topics: ['topic-A'] })
      const decoded = assignment.reduce((acc, { memberId, memberAssignment }) => {
        acc[memberId] = MemberAssignment.decode(memberAssignment).assignment
        return acc
      }, {})

      const total = Object.values(decoded).reduce((sum, a) => {
        return sum + Object.values(a).reduce((s, p) => s + p.length, 0)
      }, 0)

      expect(total).toBe(2)
    })

    test('handle single member', async () => {
      metadata['topic-A'] = Array(5)
        .fill()
        .map((_, i) => ({ partitionId: i }))

      const members = [{ memberId: 'member-1' }]

      const assignment = await assigner.assign({ members, topics: ['topic-A'] })
      const decoded = MemberAssignment.decode(assignment[0].memberAssignment)

      expect(decoded.assignment['topic-A'].sort()).toEqual([0, 1, 2, 3, 4])
    })

    test('protocol propagates prior assignment via userData after assign', async () => {
      metadata['topic-A'] = [{ partitionId: 0 }, { partitionId: 1 }]

      const members = [{ memberId: 'member-1' }]
      await assigner.assign({ members, topics: ['topic-A'] })

      const proto = assigner.protocol({ topics: ['topic-A'] })
      const decoded = MemberMetadata.decode(proto.metadata)

      expect(decoded.userData).not.toBeNull()
      expect(decoded.userData.length).toBeGreaterThan(0)

      const priorAssignment = MemberAssignment.decode(decoded.userData)
      expect(priorAssignment.assignment['topic-A'].sort()).toEqual([0, 1])
    })

    test('protocol returns empty userData before first assign', () => {
      const freshAssigner = CooperativeStickyAssigner({ cluster })
      const proto = freshAssigner.protocol({ topics: ['topic-A'] })
      const decoded = MemberMetadata.decode(proto.metadata)

      // userData should be empty buffer before any assignment
      expect(decoded.userData === null || decoded.userData.length === 0).toBe(true)
    })
  })

  describe('#protocol', () => {
    test('returns the assigner name and metadata', () => {
      const proto = assigner.protocol({ topics })
      expect(proto.name).toBe('CooperativeStickyAssigner')

      const decoded = MemberMetadata.decode(proto.metadata)
      expect(decoded.version).toBe(assigner.version)
      expect(decoded.topics).toEqual(topics)
    })
  })
})

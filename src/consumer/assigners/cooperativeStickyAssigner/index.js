const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')

/**
 * CooperativeStickyAssigner
 *
 * Uses the cooperative incremental rebalance protocol (KIP-429). Instead of
 * revoking all partitions on every rebalance (eager/stop-the-world), it only
 * moves partitions that need to be redistributed. Consumers keep their
 * existing assignments for partitions that don't need to move, reducing
 * disruption during broker maintenance and rolling deployments.
 *
 * Prior assignments are propagated via userData in the JoinGroup protocol
 * metadata, so the leader can see what each member currently owns and
 * minimize partition movement.
 *
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => {
  let currentAssignmentForProtocol = null

  return {
    name: 'CooperativeStickyAssigner',
    version: 1,

    async assign({ members, topics }) {
      const sortedMembers = members.map(({ memberId }) => memberId).sort()
      const membersCount = sortedMembers.length
      const currentAssignment = {}
      const assignment = {}

      for (const memberId of sortedMembers) {
        assignment[memberId] = Object.create(null)
      }

      for (const member of members) {
        if (member.memberMetadata) {
          try {
            const meta = MemberMetadata.decode(member.memberMetadata)
            if (meta && meta.userData && meta.userData.length > 0) {
              const decoded = MemberAssignment.decode(meta.userData)
              if (decoded && decoded.assignment) {
                currentAssignment[member.memberId] = decoded.assignment
              }
            }
          } catch (_) {} // eslint-disable-line no-empty
        }
      }

      const allPartitions = topics.flatMap(function(topic) {
        const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
        return partitionMetadata.map(function(m) {
          return { topic, partitionId: m.partitionId }
        })
      })

      const targetCount = Math.ceil(allPartitions.length / membersCount)
      const assigned = new Set()

      // Phase 1: retain existing assignments where the member still exists and
      // the partition still belongs to a subscribed topic, capping each member
      // at targetCount to allow redistribution.
      for (const memberId of sortedMembers) {
        const prior = currentAssignment[memberId]
        if (!prior) continue

        for (const topic of Object.keys(prior)) {
          if (!topics.includes(topic)) continue
          for (const partitionId of prior[topic]) {
            const key = topic + ':' + partitionId
            if (assigned.has(key)) continue

            if (!assignment[memberId][topic]) {
              assignment[memberId][topic] = []
            }

            const memberTotal = Object.values(assignment[memberId]).reduce(function(sum, p) {
              return sum + p.length
            }, 0)
            if (memberTotal >= targetCount) break

            assignment[memberId][topic].push(partitionId)
            assigned.add(key)
          }
        }
      }

      // Phase 2: distribute unassigned partitions round-robin among members
      // that have capacity below targetCount.
      const unassigned = allPartitions.filter(function(tp) {
        return !assigned.has(tp.topic + ':' + tp.partitionId)
      })

      let memberIdx = 0
      for (const tp of unassigned) {
        let attempts = 0
        while (attempts < membersCount) {
          const memberId = sortedMembers[memberIdx % membersCount]
          memberIdx++
          const memberTotal = Object.values(assignment[memberId]).reduce(function(sum, p) {
            return sum + p.length
          }, 0)

          if (memberTotal < targetCount) {
            if (!assignment[memberId][tp.topic]) {
              assignment[memberId][tp.topic] = []
            }
            assignment[memberId][tp.topic].push(tp.partitionId)
            break
          }
          attempts++
        }

        if (attempts >= membersCount) {
          const memberId = sortedMembers[memberIdx % membersCount]
          memberIdx++
          if (!assignment[memberId][tp.topic]) {
            assignment[memberId][tp.topic] = []
          }
          assignment[memberId][tp.topic].push(tp.partitionId)
        }
      }

      const result = Object.keys(assignment).map(
        function(memberId) {
          return {
            memberId,
            memberAssignment: MemberAssignment.encode({
              version: this.version,
              assignment: assignment[memberId],
            }),
          }
        }.bind(this)
      )

      // Store our own assignment so protocol() can propagate it on next rejoin
      const selfMemberId = sortedMembers.find(function(id) {
        return assignment[id] && Object.keys(assignment[id]).length > 0
      })
      if (selfMemberId) {
        currentAssignmentForProtocol = assignment[selfMemberId]
      }

      return result
    },

    protocol({ topics }) {
      const userData = currentAssignmentForProtocol
        ? MemberAssignment.encode({
            version: this.version,
            assignment: currentAssignmentForProtocol,
          })
        : Buffer.alloc(0)

      return {
        name: this.name,
        metadata: MemberMetadata.encode({
          version: this.version,
          topics,
          userData,
        }),
      }
    },
  }
}

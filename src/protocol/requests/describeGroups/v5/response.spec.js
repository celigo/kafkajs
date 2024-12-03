/* eslint-disable prettier/prettier */
const { decode, parse } = require('./response')
const { encodeDescribeGroupsResponse } = require('./response.helper')

describe('Protocol > Requests > DescribeGroups > v2', () => {
  test('response', async () => {
    const responseData = {
      throttleTime: 12,
      groups: [
        {
          errorCode: 0,
          groupId:
            'consumer-group-id-4de0aa10ef94403a397d-53384-d2fee969-1446-4166-bc8e-c88e8daffdfe',
          state: 'Stable',
          protocolType: 'protocol type',
          protocol: 'RoundRobinAssigner',
          members: [
            {
              memberId: 'member-1',
              groupInstanceId: 'group-instance-id-1',
              clientId: 'client-1',
              clientHost: 'client-host-1',
              memberMetadata: null,
              memberAssignment: null,
            },
          ],
          authorizedOperations: 0,
        },
      ],
    }
    const { buffer } = await encodeDescribeGroupsResponse(responseData)
    const data = await decode(buffer)
    expect(data).toEqual(responseData)

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
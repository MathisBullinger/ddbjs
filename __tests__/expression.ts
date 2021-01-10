import * as build from '../src/expression'

test('simple set expr', () => {
  expect(build.set({ foo: 'bar' })).toEqual({
    UpdateExpression: 'SET foo=:s0',
    ExpressionAttributeValues: { ':s0': 'bar' },
  })
})

test('reserved key', () => {
  expect(build.set({ data: 'something' })).toEqual({
    UpdateExpression: `SET #s0=:s0`,
    ExpressionAttributeValues: { ':s0': 'something' },
    ExpressionAttributeNames: { '#s0': 'data' },
  })
})

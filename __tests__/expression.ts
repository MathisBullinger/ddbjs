import * as expr from '../src/expression'

test('set expr', () => {
  expect(expr.set({ foo: 'bar' })).toEqual({
    UpdateExpression: 'SET foo=:s0',
    ExpressionAttributeValues: { ':s0': 'bar' },
  })
})

test('set reserved key', () => {
  expect(expr.set({ data: 'something' })).toEqual({
    UpdateExpression: 'SET #s0=:s0',
    ExpressionAttributeValues: { ':s0': 'something' },
    ExpressionAttributeNames: { '#s0': 'data' },
  })
})

test('remove expr', () => {
  expect(expr.remove('a', 'b', 'c')).toEqual({
    UpdateExpression: 'REMOVE a, b, c',
  })
  expect(expr.remove()).toBeUndefined()
})

test('remove reserved key', () => {
  expect(expr.remove('data')).toEqual({
    UpdateExpression: 'REMOVE #r0',
    ExpressionAttributeNames: { '#r0': 'data' },
  })
})

test('merge expressions', () => {
  expect(
    expr.merge(
      {
        UpdateExpression: 'a',
        ExpressionAttributeValues: { a: 'b' },
        ExpressionAttributeNames: { c: 'd' },
      },
      {
        UpdateExpression: 'b',
        ExpressionAttributeValues: { e: 'f' },
      }
    )
  ).toEqual({
    UpdateExpression: 'a b',
    ExpressionAttributeValues: { a: 'b', e: 'f' },
    ExpressionAttributeNames: { c: 'd' },
  })

  expect(
    expr.merge({ UpdateExpression: 'foo' }, { UpdateExpression: 'bar' })
  ).toEqual({ UpdateExpression: 'foo bar' })

  expect(() =>
    expr.merge(
      { UpdateExpression: 'a', ExpressionAttributeValues: { foo: 'a' } },
      { UpdateExpression: 'b', ExpressionAttributeValues: { foo: 'b' } }
    )
  ).toThrow()

  expect(() =>
    expr.merge(
      { UpdateExpression: 'a', ExpressionAttributeNames: { foo: 'a' } },
      { UpdateExpression: 'b', ExpressionAttributeNames: { foo: 'b' } }
    )
  ).toThrow()
})

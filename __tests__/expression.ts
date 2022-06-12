import * as expr from '../src/expression'

test('set expr', () => {
  expect(expr.set({ foo: 'bar' })).toEqual({
    UpdateExpression: 'SET foo=:s0',
    ExpressionAttributeValues: { ':s0': 'bar' },
  })
  expect(expr.set({ foo: 'bar', something: 2 })).toEqual({
    UpdateExpression: 'SET foo=:s0, something=:s1',
    ExpressionAttributeValues: { ':s0': 'bar', ':s1': 2 },
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
  expect(expr.remove('a', 'a', 'b', 'c', 'b')).toEqual({
    UpdateExpression: 'REMOVE a, b, c',
  })
  expect(expr.remove()).toBeUndefined()
})

test('remove reserved key', () => {
  expect(expr.remove('data')).toEqual({
    UpdateExpression: 'REMOVE #r0',
    ExpressionAttributeNames: { '#r0': 'data' },
  })
  expect(expr.remove('data.foo')).toEqual({
    UpdateExpression: 'REMOVE #r0.foo',
    ExpressionAttributeNames: { '#r0': 'data' },
  })
})

test('add expr', () => {
  expect(expr.add({ alpha: ['a', 'b'] })).toEqual({
    UpdateExpression: `ADD alpha :a0`,
    ExpressionAttributeValues: { ':a0': ['a', 'b'] },
  })
})

test('delete expr', () => {
  expect(expr.del({ nums: [1, 2] })).toEqual({
    UpdateExpression: 'DELETE nums :d0',
    ExpressionAttributeValues: { ':d0': [1, 2] },
  })
})

test('push in expr', () => {
  expect(
    expr.set({ nums: { [expr.fun]: 'list_append', data: [1, 2] } })
  ).toEqual({
    UpdateExpression: 'SET nums=list_append(nums, :s0)',
    ExpressionAttributeValues: { ':s0': [1, 2] },
  })

  expect(
    expr.set({ list: { [expr.fun]: 'list_append', data: [1, 2] } })
  ).toEqual({
    UpdateExpression: 'SET #s0=list_append(#s0, :s0)',
    ExpressionAttributeValues: { ':s0': [1, 2] },
    ExpressionAttributeNames: { '#s0': 'list' },
  })
})

test('projection expr', () => {
  expect(expr.project('a', 'b', 'c')).toEqual({
    ProjectionExpression: 'a, b, c',
  })
})

test('projection reserved key', () => {
  expect(expr.project('data', 'foo', 'bar')).toEqual({
    ProjectionExpression: '#p0, foo, bar',
    ExpressionAttributeNames: { '#p0': 'data' },
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

  expect(expr.merge({ ProjectionExpression: 'foo' })).toEqual({
    ProjectionExpression: 'foo',
  })

  expect(
    expr.merge({ ProjectionExpression: 'foo' }, { ProjectionExpression: 'bar' })
  ).toEqual({
    ProjectionExpression: 'foo, bar',
  })

  expect(
    expr.merge(
      { UpdateExpression: 'foo', ProjectionExpression: 'foo' },
      { UpdateExpression: 'bar' },
      { ProjectionExpression: 'bar' }
    )
  ).toEqual({
    UpdateExpression: 'foo bar',
    ProjectionExpression: 'foo, bar',
  })
})

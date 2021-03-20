import { db, ranId } from './utils/db'
import type { DBRecord } from '../src/ddb'

jest.setTimeout(20000)

// get

test('get non-existent item', async () =>
  await expect(db.get('foo')).resolves.toBeUndefined())

test('get existing item', async () =>
  await expect(db.get('bar')).resolves.toEqual({
    id: 'bar',
    data: 'something',
  }))

test('get select fields', async () => {
  const id = ranId()
  await db.put({ id, a: 'b', c: 'd', e: 'f', data: 'foo' })
  await expect(db.get(id).select('a', 'c')).resolves.toEqual({ a: 'b', c: 'd' })
  await expect(db.get(id).select('data')).resolves.toEqual({ data: 'foo' })
})

test('get (strongly consistent)', async () => {
  const obj = { id: ranId(), foo: 'bar' }
  await db.put(obj)
  await expect(db.get(obj.id)).resolves.toEqual(obj)
  await expect(db.get(obj.id).strong()).resolves.toEqual(obj)
})

// batch get

test('batch get', async () => {
  const items = Array(110)
    .fill(0)
    .map((_, i) => ({ id: `batch-${i}` }))
  await Promise.all(items.map(v => db.put(v)))

  await expect(
    db.batchGet(...items.slice(0, 10).map(({ id }) => id)).sort()
  ).resolves.toEqual(items.slice(0, 10))

  await expect(
    db.batchGet(...items.map(({ id }) => id)).sort()
  ).resolves.toEqual(items)

  await expect(
    db.batchGet('batch-1', ranId(), 'batch-2').sort()
  ).resolves.toEqual([{ id: 'batch-1' }, { id: 'batch-2' }])
})

test('batch get exceed size limit', async () => {
  const kb350 = 'a'.repeat(350 * 2 ** 10)
  const items = Array(110)
    .fill(0)
    .map((_, i) => ({ id: `batch-${i}`, kb350 }))
  await Promise.all(items.map(v => db.put(v)))

  const a = items.slice(0, 10)
  await expect(db.batchGet(...a.map(({ id }) => id)).sort()).resolves.toEqual(a)

  const b = items.slice(0, 80)
  await expect(db.batchGet(...b.map(({ id }) => id)).sort()).resolves.toEqual(b)

  await expect(
    db.batchGet(...items.map(({ id }) => id)).sort()
  ).resolves.toEqual(items)
})

test('batch get select fields', async () => {
  const items = [
    { id: ranId(), data: 'foo', a: 'b' },
    { id: ranId(), data: 'bar', c: 'd' },
  ]
  await Promise.all(items.map(v => db.put(v)))

  await expect(
    db.batchGet(...items.map(({ id }) => id)).sort()
  ).resolves.toEqual(items)

  await expect(db.batchGet(items[0].id).select('data')).resolves.toEqual([
    {
      data: 'foo',
    },
  ])

  const all = Array(200)
    .fill(0)
    .map(() => ({ id: ranId(), data: ranId() }))
  await Promise.all(all.map(v => db.put(v)))

  const pool = [...all]
  const shuffled = []
  for (let i = 0; i < 150; i++)
    shuffled.push(pool.splice((Math.random() * pool.length) | 0, 1)[0])

  await expect(
    db.batchGet(...shuffled.map(({ id }) => id)).sort()
  ).resolves.toEqual(shuffled)
  await expect(
    db
      .batchGet(...shuffled.map(({ id }) => id))
      .select('data')
      .sort()
  ).resolves.toEqual(shuffled.map(({ data }) => ({ data })))
})

// put

test('put item', async () =>
  await expect(db.put({ id: 'put-test', data: 'a' })).resolves.toBeUndefined())

test('put item (overwrite)', async () =>
  await expect(db.put({ id: 'put-test', data: 'b' })).resolves.toBeUndefined())

test('put item (return new)', async () =>
  await expect(
    db.put({ id: 'put-test', data: 'c' }).returning('NEW')
  ).resolves.toEqual({
    id: 'put-test',
    data: 'c',
  }))

test('put item (return old)', async () =>
  await expect(
    db.put({ id: 'put-test', data: 'd' }).returning('OLD')
  ).resolves.toEqual({
    id: 'put-test',
    data: 'c',
  }))

test('put item (invalid return)', () => {
  expect(() => db.put({ id: 'asdf' }).returning('UPDATED_NEW' as any)).toThrow()
})

test('put nested map', async () => {
  const id = ranId()
  const obj = { id, map: { nested: { str: 'foo' } } } as const
  await expect(db.put(obj).returning('NEW')).resolves.toEqual(obj)
})

test('set in map', async () => {
  await expect(
    db.put({ id: ranId(), map: { set: ['a', 'a'] } })
  ).rejects.toThrow()

  const item = { id: ranId(), map: { set: ['a', 'b'] } }
  await expect(db.put(item).returning('NEW')).resolves.toEqual(item)
})

test('put (if not exists)', async () => {
  await expect(db.put({ id: 'locked' }).ifNotExists()).resolves.not.toThrow()
  await expect(db.put({ id: 'locked' }).ifNotExists()).rejects.toThrow()
})

test('put null', async () => {
  await expect(db.put({ id: ranId(), abc: null as any })).resolves.not.toThrow()
})

// batch put

test('batch put', async () => {
  const items = Array(120)
    .fill(0)
    .map((_, i) => ({ id: `batch-p-${i}` }))

  await db.batchPut(...items)

  await expect(
    db.batchGet(...items.map(({ id }) => id)).sort()
  ).resolves.toEqual(items)
})

// delete

test('delete item', async () => {
  await db.put({ id: 'del1' })
  await expect(db.delete('del1')).resolves.not.toThrow()
  await expect(db.get('del1')).resolves.toBeUndefined()
})

test('delete and return', async () => {
  const obj = { id: 'del2', data: 'foo' }
  await expect(db.put(obj).returning('NEW')).resolves.toEqual(obj)
  await expect(db.delete(obj.id).returning('OLD')).resolves.toEqual(obj)
  await expect(db.get(obj.id)).resolves.toBeUndefined()
  await expect(db.delete('delnone').returning('OLD')).resolves.toBeUndefined()
})

// batch delete

test('batch delete', async () => {
  const items = Array(120)
    .fill(0)
    .map((_, i) => ({ id: `batch-d-${i}` }))
  await Promise.all(items.map(item => db.put(item)))

  await expect(
    db.batchGet(...items.map(({ id }) => id)).sort()
  ).resolves.toEqual(items)

  await db.batchDelete(...items.splice(10, 10).map(({ id }) => id))
  await expect(
    db.batchGet(...items.map(({ id }) => id)).sort()
  ).resolves.toEqual(items)

  await db.batchDelete(...items.splice(5, 101).map(({ id }) => id))
  await expect(
    db.batchGet(...items.map(({ id }) => id)).sort()
  ).resolves.toEqual(items)
})

// update

test('update item', async () => {
  const id = ranId()
  await db.put({ id, foo: 'a' })
  await expect(db.update(id, { foo: 'b', bar: 'baz' })).resolves.toBeUndefined()
  await expect(db.get(id)).resolves.toEqual({ id, foo: 'b', bar: 'baz' })
})

test('update reserved key', async () => {
  await expect(
    db.update('bar', { data: 'something else' })
  ).resolves.toBeUndefined()
  await expect(db.get('bar')).resolves.toEqual({
    id: 'bar',
    data: 'something else',
  })
})

test("can't perform empty update", async () => {
  await expect(db.update('bar')).rejects.toThrow()
})

test('update return OLD', async () => {
  await expect(
    db.update('bar', { data: 'a' }).returning('OLD')
  ).resolves.toEqual({
    id: 'bar',
    data: 'something else',
  })
})

test('update return UPDATED_OLD', async () => {
  await expect(
    db.update('bar', { data: 'b' }).returning('UPDATED_OLD')
  ).resolves.toEqual({ data: 'a' })
})

test('update return UPDATED_NEW', async () => {
  await expect(
    db.update('bar', { data: 'c' }).returning('UPDATED_NEW')
  ).resolves.toEqual({ data: 'c' })
})

test('update return UPDATED_NEW', async () => {
  await expect(
    db.update('bar', { data: 'd' }).returning('UPDATED_NEW')
  ).resolves.toEqual({ data: 'd' })
})

test('update return NEW', async () => {
  await expect(
    db.update('bar', { data: 'e' }).returning('NEW')
  ).resolves.toEqual({
    id: 'bar',
    data: 'e',
  })
})

test('update if exists', async () => {
  const id = ranId()
  await expect(
    db.update(id, { data: 'foo' }).returning('NEW')
  ).resolves.toEqual({
    id,
    data: 'foo',
  })

  await expect(
    db.update(`${id}-b`, { data: 'bar' }).ifExists()
  ).rejects.toThrow()

  await expect(
    db.update(id, { data: 'bar' }).ifExists().returning('UPDATED_NEW')
  ).resolves.toEqual({ data: 'bar' })
})

test('delete attribute', async () => {
  const obj: any = { id: ranId(), foo: 'bar', x: 'x' }
  await db.put(obj)
  delete obj.x
  await expect(
    db.update(obj.id, { $remove: ['x'] }).returning('NEW')
  ).resolves.toEqual(obj)
  await expect(db.get(obj.id)).resolves.toEqual(obj)

  obj.y = 'y'
  await expect(db.update(obj.id, { y: 'y' }).returning('NEW')).resolves.toEqual(
    obj
  )
  delete obj.y
  await expect(db.update(obj.id).remove('y').returning('NEW')).resolves.toEqual(
    obj
  )
  await expect(db.get(obj.id)).resolves.toEqual(obj)
})

// update number

test('add to number', async () => {
  await expect(
    db.put({ id: 'count', num: 1 }).returning('NEW')
  ).resolves.toMatchObject({ num: 1 })

  await expect(
    db.update('count').add({ num: 1 }).returning('UPDATED_NEW')
  ).resolves.toEqual({ num: 2 })

  await expect(
    db.update('count', { $add: { num: 2 } }).returning('UPDATED_NEW')
  ).resolves.toEqual({ num: 4 })

  await expect(
    db.update('count').add({ num: -3 }).returning('UPDATED_NEW')
  ).resolves.toEqual({ num: 1 })
})

// sets & lists

test('insert string set', async () => {
  const item = { id: 'strset', strset: ['a', 'b'] }
  await expect(db.put(item).returning('NEW')).resolves.toEqual(item)
  await expect(db.get(item.id)).resolves.toEqual(item)
})

test("can't insert duplicate in set", async () => {
  await expect(db.put({ id: 'dupset', strset: ['a', 'a'] })).rejects.toThrow()
})

test("can't insert different types in set", async () => {
  await expect(
    db.put({ id: 'mixset', strset: ['a', 2] } as any)
  ).rejects.toThrow()
})

test("can't insert empty set", async () => {
  await expect(db.put({ id: 'emptyset', strset: [] })).rejects.toThrow()
  await db.put({ id: 'emptyset', strset: ['a', 'b'] })
  await expect(db.update('emptyset', { strset: [] })).rejects.toThrow()
})

test('can insert empty list', async () => {
  await expect(db.put({ id: 'list', list: [] })).resolves.toBeUndefined()
  await expect(db.get('list')).resolves.toEqual({ id: 'list', list: [] })
})

test('explicit Set', async () => {
  await expect(
    db.put({ id: 'expset', explset: ['a'] }).cast({ explset: 'Set' })
  ).resolves.not.toThrow()
  await expect(
    db.update('expset', { explset: ['a', 'a'] }).cast({ explset: 'Set' })
  ).rejects.toThrow()

  await expect(
    db.put({ id: 'expset2', explset: ['a', 'a'] }).cast({ explset: 'Set' })
  ).rejects.toThrow()
})

test('explicit List', async () => {
  await expect(
    db.put({ id: ranId(), strset: ['a', 2] } as any).cast({ strset: 'List' })
  ).resolves.not.toThrow()
  await expect(
    db.put({ id: ranId(), strset: ['a', 2] } as any)
  ).rejects.toThrow()

  await expect(
    db
      .put({ id: ranId(), map: { set: ['a', 2] } } as any)
      .cast({ 'map.set': 'List' })
  ).resolves.not.toThrow()
  await expect(
    db.put({ id: ranId(), map: { set: ['a', 2] } } as any)
  ).rejects.toThrow()
})

test('nested cast', async () => {
  await expect(
    db.put({ id: ranId(), map: { set: ['a', 'a'] } })
  ).rejects.toThrow()
  await expect(
    db
      .put({ id: ranId(), map: { set: ['a', 'a'] } })
      .cast({ 'map.set': 'List' })
  ).resolves.not.toThrow()
})

// list manipulation

// test('list insert & delete', async () => {
//   const obj = { id: ranId(), abc: ['a', 'b', 'x', 'd'] }
//   await expect(db.put(obj).returning('NEW')).resolves.toEqual(obj)
//   await expect(
//     db.update(obj.id, { 'abc[2]': '_' }).returning('NEW')
//   ).resolves.toEqual({ abc: ['a', 'b', '_', 'd'] })
//   // await expect(
//   //   db.update(obj.id).remove('abc[2]').returning('UPDATED_NEW')
//   // ).resolves.toEqual({ abc: ['a', 'b', 'd'] })
//   await expect(db.get(obj.id)).resolves.toEqual({
//     id: obj.id,
//     abc: ['a', 'b', 'd'],
//   })
// })

// set manipulation

test('add & remove from set', async () => {
  const id = ranId()
  await db.put({ id, strset: ['a'] })

  await expect(db.get(id)).resolves.toMatchObject({ strset: ['a'] })

  await expect(
    db.update(id, { $add: { strset: ['b', 'c'] } }).returning('NEW')
  ).resolves.toMatchObject({ strset: ['a', 'b', 'c'] })

  await expect(
    db
      .update(id)
      .add({ strset: ['d', 'e'] })
      .returning('NEW')
  ).resolves.toMatchObject({ strset: ['a', 'b', 'c', 'd', 'e'] })

  await expect(
    db
      .update(ranId(), { $add: { strset: ['a', 'b'], nums: [1, 2] } })
      .returning('NEW')
  ).resolves.toMatchObject({ strset: ['a', 'b'], nums: [1, 2] })

  await expect(
    db.update(id, { $delete: { strset: ['d', 'e'] } }).returning('NEW')
  ).resolves.toMatchObject({ strset: ['a', 'b', 'c'] })

  await expect(
    db
      .update(id)
      .delete({ strset: ['c'] })
      .returning('NEW')
  ).resolves.toMatchObject({ strset: ['a', 'b'] })

  await expect(
    db
      .update(id)
      .add({ nums: [1, 2, 3] })
      .delete({ strset: ['b'] })
      .returning('NEW')
  ).resolves.toMatchObject({ strset: ['a'], nums: [1, 2, 3] })
})

// misc

test('is typescript promise', async () => {
  await db.put({ id: ranId() }).returning('NEW')
  await Promise.all([db.put({ id: ranId() })])
})

test('Record type', () => {
  const record: DBRecord<typeof db> = {
    id: 'a',
    strset: ['b', 'c'],
    bool: false,
  }
})

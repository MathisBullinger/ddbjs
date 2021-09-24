import { ranId, db, dbComp, scanDB, scanDBComp } from './utils/db'
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

  const item = await db.get(id).select('data', 'foo')
  const dataStr: string = item.data
  // @ts-expect-error
  const dataNum: number = item.data
  // @ts-expect-error
  const num = item.num
  const foo = item.foo
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
  await db.put({ id, data: 'a', num: 2 })
  await expect(db.update(id, { data: 'b', num: 3 })).resolves.toBeUndefined()
  await expect(db.get(id)).resolves.toEqual({ id, data: 'b', num: 3 })
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
  await expect(
    db.update(obj.id, { y: 'y' } as any).returning('NEW')
  ).resolves.toEqual(obj)
  delete obj.y
  await expect(db.update(obj.id).remove('y').returning('NEW')).resolves.toEqual(
    obj
  )
  await expect(db.get(obj.id)).resolves.toEqual(obj)
})

// update nested

test('update nested', async () => {
  const id = ranId()
  await db.put({ id, map: { num: 2, str: 'foo' } })

  await expect(
    db.update(id, { 'map.str': 'bar' }).returning('NEW')
  ).resolves.toEqual({ id, map: { num: 2, str: 'bar' } })

  await expect(
    db.update(id).add({ 'map.num': 1 }).returning('NEW')
  ).resolves.toEqual({ id, map: { num: 3, str: 'bar' } })
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
    db.update('expset', { explset: ['a', 'a'] } as any).cast({ explset: 'Set' })
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

// scan

const count = async ({ client, table }: any = db) => {
  const { Count } = await client.scan({ TableName: table }).promise()
  return Count
}

test('scan & truncate', async () => {
  await expect(count(scanDB)).resolves.toBe(0)

  const items = [...Array(10).keys()].map(id => ({ id }))
  await scanDB.batchPut(...items)

  const wrap = (prom: Promise<any[]>) => prom.then(v => new Set(v))

  await expect(wrap(scanDB.scan())).resolves.toEqual(new Set(items))

  const newItems = new Array(1000)
    .fill(0)
    .map((_, i) => ({ id: i + 10, payload: '_'.repeat(1e4) }))
  await scanDB.batchPut(...newItems)
  items.push(...newItems)

  await expect(wrap(scanDB.scan())).resolves.toEqual(new Set(items))

  for (let limit of [3, 900]) {
    const res = await scanDB.scan().limit(limit)
    expect(res.length).toBe(limit)
    expect(items).toEqual(expect.arrayContaining(res))
  }

  // projection
  await expect(
    scanDB.scan().then(res => new Set(res.flatMap(v => Object.keys(v))))
  ).resolves.toEqual(new Set(['id', 'payload']))

  await expect(
    scanDB
      .scan()
      .select('id')
      .then(res => new Set(res.flatMap(v => Object.keys(v))))
  ).resolves.toEqual(new Set(['id']))

  // truncate
  await expect(count(scanDB)).resolves.toBeGreaterThan(0)
  await expect(scanDB.truncate()).resolves.not.toThrow()
  await expect(count(scanDB)).resolves.toBe(0)

  await scanDBComp.batchPut(
    ...[...Array(5000)].map(() => ({
      pk: Math.random().toString(),
      sk: Math.random().toString(),
      [`a${Math.random()}`]: Math.random(),
    }))
  )
  await expect(count(scanDBComp)).resolves.toBeGreaterThan(0)
  await expect(scanDBComp.truncate()).resolves.not.toThrow()
  await expect(count(scanDBComp)).resolves.toBe(0)
})

// condition expressions

test('conditions', async () => {
  {
    const id = ranId()
    await db.put({ id, bool: true, num: 0 })

    await expect(
      db.update(id, { num: 1 }).if('bool', '=', false).debug()
    ).rejects.toThrow()

    await expect(
      db.update(id, { num: 2 }).if('bool', '=', false)
    ).rejects.toThrow()

    await expect(
      db.update(id, { num: 3 }).if('bool', '<>', true)
    ).rejects.toThrow()

    await expect(db.get(id)).resolves.toMatchObject({ num: 0 })

    await expect(
      db.update(id, { num: 4 }).if('bool', '=', true)
    ).resolves.toBeUndefined()

    await expect(db.get(id)).resolves.toMatchObject({ num: 4 })
  }

  {
    const id = ranId()
    await db.put({ id, num: 5 })

    await expect(db.update(id, { num: 6 }).if('num', '=', 7)).rejects.toThrow()
    await expect(
      db.update(id, { num: 6 }).if('num', '=', 7).orIf('num', '=', 5)
    ).resolves.not.toThrow()
  }

  {
    const id = ranId()
    await db.put({ id, data: 'foo', num: 1 })

    // (false and true) or true
    await expect(
      db
        .update(id, { data: 'baz' })
        .if('data', '=', 'bar')
        .andIf('num', '>', 0)
        .orIf('num', '<=', 1)
    ).resolves.not.toThrow()

    // false and (true or true)
    await expect(
      db
        .update(id, { data: 'baz' })
        .if('data', '=', 'bar')
        .andIf(v => v.if('num', '>', 0).orIf('num', '<=', 1))
    ).rejects.toThrow()
  }

  {
    const id = ranId()
    await db.put({ id, data: 'foo' })

    await expect(
      db.update(id, { bool: true }).if.not('data', '=', 'foo')
    ).rejects.toThrow()

    await expect(
      db.update(id, { bool: true }).if.not.not('data', '=', 'foo')
    ).resolves.not.toThrow()

    await expect(
      db.update(id, { bool: true }).if.not.not.not('data', '=', 'foo')
    ).rejects.toThrow()

    await expect(
      db.update(id, { bool: true }).if.not.not.not.not('data', '=', 'foo')
    ).resolves.not.toThrow()

    await expect(
      db
        .update(id, { bool: true })
        .if.not(v => v.if('data', '=', 'bar').orIf('data', '=', 'baz'))
    ).resolves.not.toThrow()

    await expect(
      db
        .update(id, { bool: true })
        .if.not('data', '=', 'foo')
        .orIf.not('data', '<>', 'foo')
    ).resolves.not.toThrow()
  }

  {
    const id = ranId()
    await db.put({ id, data: 'foo' })

    await expect(
      db.update(id, { num: 0 }).if.attributeExists('bool')
    ).rejects.toThrow()
    await expect(
      db.update(id, { num: 1 }).if.attributeExists('data')
    ).resolves.not.toThrow()

    await expect(
      db
        .update(id, { num: 2 })
        .if.attributeExists('bool')
        .andIf.attributeExists('data')
    ).rejects.toThrow()

    await expect(
      db
        .update(id, { num: 3 })
        .if.attributeExists('bool')
        .orIf.attributeExists('data')
    ).resolves.not.toThrow()

    await expect(
      db.update(id, { num: 4 }).if.not.attributeExists('bool')
    ).resolves.not.toThrow()
    await expect(
      db.update(id, { num: 5 }).if.not(v => v.if.attributeExists('bool'))
    ).resolves.not.toThrow()

    await expect(
      db.update(id, { num: 6 }).if.not.attributeExists('data')
    ).rejects.toThrow()
    await expect(
      db.update(id, { num: 7 }).if.not(v => v.if.attributeExists('data'))
    ).rejects.toThrow()
  }

  {
    const id = ranId()
    await db.put({
      id,
      data: 'abc',
      bool: false,
      num: 1,
      strset: ['a', 'b', 'c'],
    })

    await expect(
      db.update(id, { num: 2 }).if.attributeExists('data')
    ).resolves.not.toThrow()
    await expect(
      db.update(id, { num: 2 }).if.attributeExists('abc')
    ).rejects.toThrow()

    await expect(
      db.update(id, { num: 2 }).if.attributeNotExists('data')
    ).rejects.toThrow()
    await expect(
      db.update(id, { num: 2 }).if.attributeNotExists('abc')
    ).resolves.not.toThrow()

    await expect(
      db.update(id, { num: 2 }).if.attributeType('data', 'S')
    ).resolves.not.toThrow()
    await expect(
      db.update(id, { num: 2 }).if.attributeType('data', 'N')
    ).rejects.toThrow()

    await expect(
      db.update(id, { num: 2 }).if.beginsWith('data', 'ab')
    ).resolves.not.toThrow()
    await expect(
      db.update(id, { num: 2 }).if.beginsWith('data', 'bc')
    ).rejects.toThrow()

    await expect(
      db.update(id, { num: 2 }).if.contains('data', 'bc')
    ).resolves.not.toThrow()
    await expect(
      db.update(id, { num: 2 }).if.contains('data', 'cd')
    ).rejects.toThrow()

    await expect(
      db.update(id, { num: 2 }).if.contains('strset', 'b')
    ).resolves.not.toThrow()
    await expect(
      db.update(id, { num: 2 }).if.contains('strset', 'd')
    ).rejects.toThrow()

    await expect(
      db.update(id, { num: 2 }).if({ size: 'data' }, '>=', 3)
    ).resolves.not.toThrow()
    await expect(
      db.update(id, { num: 2 }).if({ size: 'strset' }, '>', 20)
    ).rejects.toThrow()
  }

  {
    const id = ranId()
    await db.put({ id, data: 'foo', strset: ['a', 'b'], subStr: 'fo' })

    await expect(
      db
        .update(id, { bool: true })
        .if({ size: 'data' }, '>', { size: 'strset' })
    ).resolves.not.toThrow()

    await expect(
      db
        .update(id, { bool: true })
        .if({ size: 'data' }, '<=', { size: 'strset' })
    ).rejects.toThrow()

    await expect(
      db.update(id, { bool: true }).if(2, '=', { size: 'strset' })
    ).resolves.not.toThrow()
  }

  {
    const id = ranId()
    await db.put({ id, num: 5, num2: 5 })

    await expect(
      db.update(id, { bool: true }).if('num', 'between', 10, 15)
    ).rejects.toThrow()
    await expect(
      db.update(id, { bool: true }).if('num', 'between', 3, 7)
    ).resolves.not.toThrow()

    await expect(
      db.update(id, { bool: true }).if(7, 'between', 'num', 10)
    ).resolves.not.toThrow()
    await expect(
      db.update(id, { bool: true }).if(15, 'between', 'num', 10)
    ).rejects.toThrow()

    await expect(
      db.update(id, { bool: true }).if('num', 'in', 4, 5, 6)
    ).resolves.not.toThrow()
    await expect(
      db.update(id, { bool: true }).if('num', 'in', 6, 7, 'a')
    ).rejects.toThrow()
    await expect(
      db.update(id, { bool: true }).if('num', 'in', { path: 'num2' as any })
    ).resolves.not.toThrow()
  }

  {
    const id = ranId()
    await db.put({ id, num: 1, numA: 5, numB: 6 })

    await expect(
      db
        .update(id)
        .add({ num: 1 })
        .if({ path: '' }, '<', 5)
        .andIf({ path: 'numB' }, '>', 5)
    ).rejects.toThrow()
    await expect(db.get(id).select('num')).resolves.toEqual({ num: 1 })

    await expect(
      db
        .update(id)
        .add({ num: 1 })
        .if({ path: 'numA' }, '<=', 5)
        .andIf({ path: 'numB' }, '>', 5)
    ).resolves.not.toThrow()
    await expect(db.get(id).select('num')).resolves.toEqual({ num: 2 })
  }
})

test('nested condition operand', async () => {
  const id = ranId()
  const map = { str: 'foo', num: 2 }
  await db.put({ id, map })

  await expect(db.delete(id).if('map.str', '<>', 'foo')).rejects.toThrow()
  await expect(db.get(id)).resolves.toEqual({ id, map })
  await expect(db.delete(id).if('map.str', '=', 'foo')).resolves.not.toThrow()
  await expect(db.get(id)).resolves.toEqual(undefined)

  // strict path typing disabled for now to allow for dynamic paths
  // await db
  //   .delete(id)
  //   .if({ path: 'map.str' }, '=', 'foo')
  //   .catch(() => {})
  // await db
  //   .delete(id)
  //   // @ts-expect-error
  //   .if({ path: 'bool.str' }, '=', 'foo')
  //   .catch(() => {})
})

// query

test('query', async () => {
  {
    const id = ranId()
    await db.put({ id })
    await expect(db.query(id)).resolves.toEqual([{ id }])
  }

  {
    const pk = ranId()
    const sks = ['a', 'b', 'foo', 'bar']
    await dbComp.batchPut(...sks.map(sk => ({ pk, sk })))

    await expect(dbComp.query(pk)).resolves.toHaveLength(sks.length)

    await expect(dbComp.query(pk).where('=', 'foo')).resolves.toHaveLength(1)
    await expect(dbComp.query(pk).where['=']('foo')).resolves.toHaveLength(1)

    await expect(dbComp.query(pk).where('<', 'c')).resolves.toHaveLength(3)
    await expect(dbComp.query(pk).where['<']('c')).resolves.toHaveLength(3)

    await expect(dbComp.query(pk).where('>=', 'b')).resolves.toHaveLength(3)
    await expect(dbComp.query(pk).where['>=']('b')).resolves.toHaveLength(3)

    await expect(
      dbComp.query(pk).where('begins_with', 'b')
    ).resolves.toHaveLength(2)
    await expect(dbComp.query(pk).where.beginsWith('b')).resolves.toHaveLength(
      2
    )

    await expect(
      dbComp.query(pk).where('between', 'c', 'z')
    ).resolves.toHaveLength(1)
    await expect(
      dbComp.query(pk).where.between('c', 'z')
    ).resolves.toHaveLength(1)

    const [item] = await dbComp.query(pk).select('sk')
    expect(Object.keys(item)).toEqual(['sk'])
  }
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

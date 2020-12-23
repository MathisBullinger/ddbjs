import { DDB } from '../src'
import * as localDynamo from 'local-dynamo'
import { DynamoDB } from 'aws-sdk'

const TableName = `test-ddb-${Date.now()}`

const ddbOpts = {
  region: 'localhost',
  endpoint: 'http://localhost:4567',
}

const ddb = new DynamoDB(ddbOpts)
const db = new DDB(
  TableName,
  { key: 'id', id: String, data: String, strset: [String], list: [] },
  ddbOpts
)

beforeAll(async () => {
  localDynamo.launch(undefined, 4567)
  await ddb
    .createTable({
      AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1,
      },
      TableName,
    })
    .promise()

  await db.client
    .put({ TableName, Item: { id: 'bar', data: 'something' } })
    .promise()
})

afterAll(async () => {
  await ddb.deleteTable({ TableName }).promise()
})

// get

test('get non-existent item', async () =>
  await expect(db.get('foo')).resolves.toBeUndefined())
test('get existing item', async () =>
  await expect(db.get('bar')).resolves.toEqual({
    id: 'bar',
    data: 'something',
  }))

// insert

test('insert item', async () =>
  await expect(db.put({ id: 'put-test', data: 'a' })).resolves.toBeUndefined())

test('insert item (overwrite)', async () =>
  await expect(db.put({ id: 'put-test', data: 'b' })).resolves.toBeUndefined())

test('insert item (return new)', async () =>
  await expect(
    db.put({ id: 'put-test', data: 'c' }).returning('NEW')
  ).resolves.toEqual({
    id: 'put-test',
    data: 'c',
  }))

test('insert item (return old)', async () =>
  await expect(
    db.put({ id: 'put-test', data: 'd' }).returning('OLD')
  ).resolves.toEqual({
    id: 'put-test',
    data: 'c',
  }))

test('insert item (invalid return)', () => {
  expect(() => db.put({ id: 'asdf' }).returning('UPDATED_NEW')).toThrow()
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

// update

test('update item', async () =>
  expect(await db.update('bar', { data: 'something else' })).toBeUndefined())

test('item was updated', async () =>
  expect(await db.get('bar')).toEqual({
    id: 'bar',
    data: 'something else',
  }))

test('update return OLD', async () => {
  expect(await db.update('bar', { data: 'a' }).returning('OLD')).toEqual({
    id: 'bar',
    data: 'something else',
  })
})

test('update return UPDATED_OLD', async () => {
  expect(
    await db.update('bar', { data: 'b' }).returning('UPDATED_OLD')
  ).toEqual({ data: 'a' })
})

test('update return UPDATED_NEW', async () => {
  expect(
    await db.update('bar', { data: 'c' }).returning('UPDATED_NEW')
  ).toEqual({ data: 'c' })
})

test('update return UPDATED_NEW', async () => {
  expect(
    await db.update('bar', { data: 'd' }).returning('UPDATED_NEW')
  ).toEqual({ data: 'd' })
})

test('update return NEW', async () => {
  expect(await db.update('bar', { data: 'e' }).returning('NEW')).toEqual({
    id: 'bar',
    data: 'e',
  })
})

test('update if exists', async () => {
  const id = `id-${Date.now()}`
  expect(db.update(id, { data: 'foo' }).returning('NEW')).resolves.toEqual({
    id,
    data: 'foo',
  })

  await expect(
    db.update(`${id}-b`, { data: 'bar' }).ifExists()
  ).rejects.toThrow()

  expect(
    db.update(id, { data: 'bar' }).ifExists().returning('UPDATED_NEW')
  ).resolves.toEqual({ data: 'bar' })
})

// sets & lists

test('insert string set', async () => {
  const item = { id: 'strset', strset: ['a', 'b'] } as const
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
  // @ts-ignore
  expect(db.put({ id: 'emptyset', strset: [] })).rejects.toThrow()
  await db.put({ id: 'emptyset', strset: ['a', 'b'] })
  // @ts-ignore
  expect(db.update('emptyset', { strset: [] })).rejects.toThrow()
})

test('can insert empty list', async () => {
  expect(await db.put({ id: 'list', list: [] })).toBeUndefined()
  expect(await db.get('list')).toEqual({ id: 'list', list: [] })
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
    db.put({ id: 'explist', strset: ['a', 2] } as any).cast({ strset: 'List' })
  ).resolves.not.toThrow()
  await expect(
    db.put({ id: 'explist', strset: ['a', 2] } as any)
  ).rejects.toThrow()
})

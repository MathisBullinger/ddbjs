import { DDB } from '../src'
import * as localDynamo from 'local-dynamo'
import { DynamoDB } from 'aws-sdk'

const TableName = `test-ddb-${Date.now()}`

const ddbOpts = {
  region: 'localhost',
  endpoint: 'http://localhost:4567',
}

const ddb = new DynamoDB(ddbOpts)
const db = new DDB(TableName, { key: 'id', id: String, data: String }, ddbOpts)

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

test('get non-existent item', async () =>
  expect(await db.get('foo')).toBeUndefined())
test('get existing item', async () =>
  expect(await db.get('bar')).toMatchObject({ id: 'bar', data: 'something' }))

test('insert item', async () =>
  expect(await db.insert({ id: 'put-test', data: 'a' })).toBeUndefined())

test('insert item (overwrite)', async () =>
  expect(await db.insert({ id: 'put-test', data: 'b' })).toBeUndefined())

test('insert item (return new)', async () =>
  expect(
    await db.insert({ id: 'put-test', data: 'c' }).returning('NEW')
  ).toMatchObject({ id: 'put-test', data: 'c' }))

test('insert item (return old)', async () =>
  expect(
    await db.insert({ id: 'put-test', data: 'd' }).returning('OLD')
  ).toMatchObject({ id: 'put-test', data: 'c' }))

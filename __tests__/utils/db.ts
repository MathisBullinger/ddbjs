import { DDB } from '../../src'
import * as localDynamo from 'local-dynamo'
import { DynamoDB } from 'aws-sdk'

export const ranId = () => ((Math.random() * 1e6) | 0).toString(16)

export const TableName = `test-ddb-${Date.now()}`

export const opts = {
  region: 'localhost',
  endpoint: 'http://localhost:4567',
}

export const ddb = new DynamoDB(opts)

export const db = new DDB(
  TableName,
  { key: 'id', id: String, data: String, strset: [String], list: [], abc: [] },
  opts
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

import { DDB } from '../../src'
import * as localDynamo from 'local-dynamo'
import * as AWS from 'aws-sdk'

export const ranId = () => ((Math.random() * 1e6) | 0).toString(16)

AWS.config.update({
  region: '========',
  accessKeyId: '========',
  secretAccessKey: '========',
})

export const TableName = `test-ddb-${Date.now()}`

export const opts = {
  region: 'localhost',
  endpoint: 'http://localhost:4567',
}

export const ddb = new AWS.DynamoDB(opts)

export const db = new DDB(
  TableName,
  {
    key: 'id',
    id: String,
    data: String,
    num: Number,
    bool: Boolean,
    strset: [String],
    list: [],
    abc: [],
    map: {
      str: String,
      num: Number,
      list: [],
      set: [String],
      nested: { foo: String },
    },
  },
  opts
)

let child: ReturnType<typeof localDynamo.launch>

beforeAll(async () => {
  child = localDynamo.launch(undefined, 4567)
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
  child?.kill()
})

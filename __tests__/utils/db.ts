import { DDB, DDBKey } from '../../src'
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
    [DDBKey]: 'id',
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

export const scanDB = new DDB(
  `scan-${TableName}`,
  { [DDBKey]: 'id', id: Number },
  opts
)

export const scanDBComp = new DDB(
  `scan-${TableName}-comp`,
  { [DDBKey]: ['pk', 'sk'], pk: String, sk: String },
  opts
)

let child: ReturnType<typeof localDynamo.launch>

async function createTable(
  TableName: string,
  ...keys: [name: string, type: string, role: string][]
) {
  await ddb
    .createTable({
      AttributeDefinitions: keys.map(([name, type]) => ({
        AttributeName: name,
        AttributeType: type,
      })),
      KeySchema: keys.map(([name, , type]) => ({
        AttributeName: name,
        KeyType: type,
      })),
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1,
      },
      TableName,
    })
    .promise()
}

beforeAll(async () => {
  child = localDynamo.launch(undefined, 4567)
  await Promise.all([
    createTable(db.table, ['id', 'S', 'HASH']),
    createTable(scanDB.table, ['id', 'N', 'HASH']),
    createTable(scanDBComp.table, ['pk', 'S', 'HASH'], ['sk', 'S', 'RANGE']),
  ])
  await db.client
    .put({ TableName, Item: { id: 'bar', data: 'something' } })
    .promise()
})

afterAll(async () => {
  await ddb.deleteTable({ TableName }).promise()
  await ddb.deleteTable({ TableName: scanDB.table }).promise()
  await ddb.deleteTable({ TableName: scanDBComp.table }).promise()
  child?.kill()
})

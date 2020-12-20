import * as AWS from 'aws-sdk'
import { PutChain, UpdateChain } from './chain'

export class DDB<T extends Schema<F>, F extends Fields = Omit<T, 'key'>> {
  public readonly client: AWS.DynamoDB.DocumentClient
  private readonly fields: F

  constructor(
    public readonly table: string,
    private readonly schema: T,
    opts?: ConstructorParameters<typeof AWS.DynamoDB.DocumentClient>[0]
  ) {
    this.client = new AWS.DynamoDB.DocumentClient(opts)
    this.fields = Object.fromEntries(
      Object.entries(schema).filter(([k]) => k !== 'key')
    ) as F
  }

  public async get(
    ...key: KeyValue<T, F>
  ): Promise<Item<F, T['key']> | undefined> {
    const { Item } = await this.client
      .get({
        TableName: this.table,
        Key: this.key(...key),
      })
      .promise()

    return Item as any
  }

  public insert(item: Item<F, T['key']>) {
    const params: AWS.DynamoDB.DocumentClient.PutItemInput = {
      TableName: this.table,
      Item: item,
    }
    return new PutChain(this.fields, this.client, params, 'NONE')
  }

  public update(
    key: FlatKeyValue<T, F>,
    update: AtLeastOne<Item<F, T['key']>>
  ) {
    const ExpressionAttributeNames: Record<string, string> = {}
    const ExpressionAttributeValues: Record<string, any> = {}
    const sets: [string, string][] = []

    for (const [k, v] of Object.entries(update)) {
      const encKey = DDB.encode(k)
      ExpressionAttributeNames[`#${encKey}`] = k
      ExpressionAttributeValues[`:${encKey}`] = v
      sets.push([`#${encKey}`, `:${encKey}`])
    }

    const UpdateExpression = `SET ${sets.map(v => v.join('=')).join(', ')}`

    const params: AWS.DynamoDB.DocumentClient.UpdateItemInput = {
      TableName: this.table,
      Key: this.key(
        ...((typeof key === 'string' ? [key] : key) as KeyValue<T, F>)
      ),
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      UpdateExpression,
    }

    return new UpdateChain(this.fields, this.client, params, 'NONE')
  }

  private key(...v: KeyValue<T, F>) {
    return Object.fromEntries(
      (typeof this.schema.key === 'string'
        ? [this.schema.key]
        : (this.schema.key as string[])
      ).map((k, i) => [k, v[i]])
    )
  }

  public static encode(v: string) {
    return Buffer.from(v).toString('hex')
  }

  public static decode(v: string) {
    return Buffer.from(v, 'hex').toString()
  }
}

import * as AWS from 'aws-sdk'
import { PutChain, UpdateChain } from './chain'
import { ValidationError } from './utils/error'

export class DDB<T extends Schema<F>, F extends Fields = Omit<T, 'key'>> {
  public readonly client: AWS.DynamoDB.DocumentClient
  private readonly fields: F

  /**
   * @example
   * new DDB('users', {
   *   key: 'id', id: String, name: String, tags: [Number]
   * })
   *
   * @param table table name
   * @param schema Schema of the table. Must include the key.
   * @param opts parameters passed to DynamoDB document client
   */
  constructor(
    public readonly table: string,
    private readonly schema: T,
    params?: ConstructorParameters<typeof AWS.DynamoDB.DocumentClient>[0]
  ) {
    this.client = new AWS.DynamoDB.DocumentClient(params)
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

  public insert<I extends Item<F, T['key']>>(item: I, types?: ExplTypes<I>) {
    this.validate(item)

    const keys = Object.keys(item) as (keyof Item<F, T['key']>)[]
    for (const k of keys)
      if (
        (this.isSet(k as string) && types?.[k] !== 'List') ||
        types?.[k] === 'Set'
      )
        item[k as keyof Item<F, T['key']>] = this.createSet(
          item[k] as any
        ) as any

    const params: AWS.DynamoDB.DocumentClient.PutItemInput = {
      TableName: this.table,
      Item: item,
    }
    return new PutChain(this.fields, this.client, params, 'NONE')
  }

  public update<U extends ItemUpdate<T, F>>(
    key: FlatKeyValue<T, F>,
    update: NotEmptyObj<U>,
    types?: ExplTypes<U>
  ) {
    this.validate(update as any)

    const ExpressionAttributeNames: Record<string, string> = {}
    const ExpressionAttributeValues: Record<string, any> = {}
    const sets: [string, string][] = []

    const Key = this.key(
      ...((typeof key === 'string' ? [key] : key) as KeyValue<T, F>)
    )

    for (const [k, v] of Object.entries(update)) {
      const encKey = DDB.encode(k)
      ExpressionAttributeNames[`#${encKey}`] = k
      if (
        (this.isSet(k) && types?.[k as keyof U] !== 'List') ||
        types?.[k as keyof U] === 'Set'
      )
        ExpressionAttributeValues[`:${encKey}`] = this.createSet(v as any)
      else ExpressionAttributeValues[`:${encKey}`] = v
      sets.push([`#${encKey}`, `:${encKey}`])
    }

    const UpdateExpression = `SET ${sets.map(v => v.join('=')).join(', ')}`

    const params: AWS.DynamoDB.DocumentClient.UpdateItemInput = {
      TableName: this.table,
      Key,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      UpdateExpression,
    }

    return new UpdateChain(this.fields, this.client, params, 'NONE')
  }

  public static encode(v: string) {
    return Buffer.from(v).toString('hex')
  }

  public static decode(v: string) {
    return Buffer.from(v, 'hex').toString()
  }

  private key(...v: KeyValue<T, F>) {
    return Object.fromEntries(
      (typeof this.schema.key === 'string'
        ? [this.schema.key]
        : (this.schema.key as string[])
      ).map((k, i) => [k, v[i]])
    )
  }

  private validate(item: Partial<Item<F, T['key']>>) {
    for (const [k, v] of Object.entries(item)) {
      if (this.isSet(k) && (!Array.isArray(v) || v.length === 0))
        throw new ValidationError(`attribute '${k}' can't contain an empty set`)
    }
  }

  private isSet(attr: string): boolean {
    const type = this.schema[attr]
    return Array.isArray(type) && [String, Number].includes(type[0] as any)
  }

  private createSet(
    items: string[] | number[]
  ): AWS.DynamoDB.DocumentClient.DynamoDbSet {
    return this.client.createSet(items)
  }
}

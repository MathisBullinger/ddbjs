import * as AWS from 'aws-sdk'
import {
  PutChain,
  DeletionChain,
  UpdateChain,
  BatchGetChain,
  GetChain,
} from './chain'
import type {
  Schema,
  Fields,
  KeyValue,
  Item,
  ItemUpdate,
  FlatKeyValue,
} from './types'

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
    params?:
      | ConstructorParameters<typeof AWS.DynamoDB.DocumentClient>[0]
      | AWS.DynamoDB.DocumentClient
  ) {
    this.client =
      params instanceof AWS.DynamoDB.DocumentClient
        ? params
        : new AWS.DynamoDB.DocumentClient(params)
    this.fields = Object.fromEntries(
      Object.entries(schema).filter(([k]) => k !== 'key')
    ) as F
  }

  public get(...key: KeyValue<T, F>): GetChain<F> {
    return new GetChain(this.fields, this.client, this.table, this.key(...key))
  }

  public batchGet(...keys: FlatKeyValue<T, F>[]): BatchGetChain<T, F> {
    return new BatchGetChain(
      this.schema,
      this.client,
      this.table,
      keys.map(key =>
        this.key(...((typeof key === 'string' ? [key] : key) as KeyValue<T, F>))
      )
    )
  }

  public put<I extends Item<F, T['key']>>(item: I): PutChain<F, 'NONE'> {
    const params: AWS.DynamoDB.DocumentClient.PutItemInput = {
      TableName: this.table,
      Item: item,
    }
    return new PutChain(this.fields, this.client, params)
  }

  public delete(...key: KeyValue<T, F>): DeletionChain<F, 'NONE'> {
    const params: AWS.DynamoDB.DocumentClient.DeleteItemInput = {
      TableName: this.table,
      Key: this.key(...key),
    }
    return new DeletionChain(this.fields, this.client, params, 'NONE')
  }

  public update<U extends ItemUpdate<T, F>>(
    key: FlatKeyValue<T, F>,
    update?: U
  ): UpdateChain<T, 'NONE', F> {
    const remove = update?.$remove
    delete update?.$remove

    return new UpdateChain(this.fields, this.client, {
      table: this.table,
      key: this.key(
        ...((typeof key === 'string' ? [key] : key) as KeyValue<T, F>)
      ),
      set: update,
      remove,
    })
  }

  private key(...v: KeyValue<T, F>) {
    return Object.fromEntries(
      (typeof this.schema.key === 'string'
        ? [this.schema.key]
        : (this.schema.key as string[])
      ).map((k, i) => [k, v[i]])
    )
  }
}

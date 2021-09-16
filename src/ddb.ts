import * as AWS from 'aws-sdk'
import * as chain from './chain'
import omit from 'snatchblock/omit'
import type {
  Schema,
  Fields,
  KeyValue,
  Item,
  ItemUpdate,
  FlatKeyValue,
  DBItem,
} from './types'

export const DDBKey = Symbol('key')
chain.BaseChain.key = DDBKey

export class DDB<
  T extends Schema<F>,
  F extends Fields = Omit<T, typeof DDBKey>
> {
  public readonly client: AWS.DynamoDB.DocumentClient
  private readonly fields: F
  public static key: typeof DDBKey = DDBKey

  /**
   * @example
   * new DDB('users', {
   *   [DDBKey]: 'id', id: String, name: String, tags: [Number]
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
    this.fields = omit(schema, DDBKey) as any
    this.keyValue = this.keyValue.bind(this)
  }

  public get(...key: KeyValue<T, F>): chain.Get<F> {
    return new chain.Get(
      this.fields,
      this.client,
      this.table,
      this.buildKey(...key)
    )
  }

  public batchGet(...keys: FlatKeyValue<T, F>[]): chain.BatchGet<T, F> {
    return new chain.BatchGet(
      this.schema,
      this.client,
      this.table,
      keys.map(key =>
        this.buildKey(
          ...((typeof key === 'string' ? [key] : key) as KeyValue<T, F>)
        )
      )
    )
  }

  public put<I extends Item<F, T[typeof DDBKey]>>(
    item: I
  ): chain.Put<F, 'NONE'> {
    const params: AWS.DynamoDB.DocumentClient.PutItemInput = {
      TableName: this.table,
      Item: item,
    }
    return new chain.Put(this.fields, this.client, this.keyFields, params)
  }

  public batchPut(...items: Item<F, T[typeof DDBKey]>[]): chain.BatchPut<T, F> {
    return new chain.BatchPut(this.schema, this.client, this.table, items)
  }

  public delete(...key: KeyValue<T, F>): chain.Delete<F, 'NONE'> {
    const params: AWS.DynamoDB.DocumentClient.DeleteItemInput = {
      TableName: this.table,
      Key: this.buildKey(...key),
    }
    return new chain.Delete(this.fields, this.client, params, 'NONE')
  }

  public batchDelete(...keys: FlatKeyValue<T, F>[]): chain.BatchDelete<T> {
    return new chain.BatchDelete(
      this.schema,
      this.client,
      this.table,
      keys.map(key =>
        this.buildKey(...((Array.isArray(key) ? key : [key]) as KeyValue<T, F>))
      )
    )
  }

  public update<U extends ItemUpdate<T, F>>(
    key: FlatKeyValue<T, F>,
    update?: U
  ): chain.Update<T, 'NONE', F> {
    const remove: string[] = (update as any)?.$remove
    delete (update as any)?.$remove
    const add = (update as any)?.$add
    delete (update as any)?.$add
    const del = (update as any)?.$delete
    delete (update as any)?.$delete

    return new (chain.Update as any)(this.fields, this.client, {
      table: this.table,
      key: this.buildKey(
        ...((typeof key === 'string' ? [key] : key) as KeyValue<T, F>)
      ),
      set: update,
      remove,
      add,
      delete: del,
    })
  }

  public scan(): chain.Scan<F> {
    return new chain.Scan(this.fields, this.client, this.table)
  }

  public query(partitionKey: KeyValue<T, F>[0]): chain.Query<T, F> {
    return new chain.Query(this.schema, this.client, this.table, partitionKey)
  }

  public async truncate() {
    const items = await this.scan().select(...this.keyFields)
    await this.batchDelete(...items.map(this.keyValue))
  }

  private get keyFields(): string[] {
    return typeof this.schema[DDBKey] === 'string'
      ? [this.schema[DDBKey] as string]
      : (this.schema[DDBKey] as string[])
  }

  private buildKey(...v: KeyValue<T, F>) {
    return Object.fromEntries(this.keyFields.map((k, i) => [k, v[i]]))
  }

  private keyValue(obj: DBItem<F>): FlatKeyValue<T, F> {
    const [h, s] = this.keyFields.map(k => obj[k])
    return s !== undefined ? [h, s] : h
  }
}

export type DBRecord<T extends DDB<any, any>> = T extends DDB<infer S, infer F>
  ? Item<F, S[typeof DDBKey]>
  : never

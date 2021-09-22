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
  ScItem,
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

  private config = <T>(cst: T) => ({
    schema: this.schema,
    client: this.client,
    table: this.table,
    ...cst,
  })

  public get = (...key: KeyValue<T, F>) =>
    new chain.Get(this.config({ key: this.buildKey(...key) }))

  public batchGet = (...keys: FlatKeyValue<T, F>[]) =>
    new chain.BatchGet(
      this.config({
        keys: keys.map(key =>
          this.buildKey(
            ...((typeof key === 'string' ? [key] : key) as KeyValue<T, F>)
          )
        ),
      })
    )

  public put = <I extends ScItem<T>>(item: I) =>
    new chain.Put(
      this.config({
        return: 'NONE',
        item,
      })
    )

  public batchPut = (...items: ScItem<T>[]) =>
    new chain.BatchPut(this.config({ items }))

  public delete = (...key: KeyValue<T, F>) =>
    new chain.Delete(
      this.config({
        return: 'NONE',
        key: this.buildKey(...key),
      })
    )

  public batchDelete = (...keys: FlatKeyValue<T, F>[]) =>
    new chain.BatchDelete(
      this.config({
        keys: keys.map(key =>
          this.buildKey(
            ...((Array.isArray(key) ? key : [key]) as KeyValue<T, F>)
          )
        ),
      })
    )

  public update<U extends ItemUpdate<T, F>>(
    key: FlatKeyValue<T, F>,
    update?: U
  ): chain.Update<T, 'NONE'> {
    const remove: string[] = (update as any)?.$remove
    delete (update as any)?.$remove
    const add = (update as any)?.$add
    delete (update as any)?.$add
    const del = (update as any)?.$delete
    delete (update as any)?.$delete

    // FIXME: type instantiation
    return new (chain.Update as any)(
      this.config({
        return: 'NONE',
        key: this.buildKey(
          ...((typeof key === 'string' ? [key] : key) as KeyValue<T, F>)
        ),
        update: { set: update, remove, add, delete: del },
      })
    )
  }

  public scan = () => new chain.Scan(this.config({}))

  public query = (partitionKey: KeyValue<T, F>[0]) =>
    new chain.Query(this.config({ key: partitionKey }))

  public async truncate() {
    const items: any[] = await this.scan().select(...this.keyFields)
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

  private keyValue(obj: ScItem<T>): FlatKeyValue<T, F> {
    const [h, s] = this.keyFields.map(k => (obj as any)[k])
    return s !== undefined ? [h, s] : h
  }
}

export type DBRecord<T extends DDB<any, any>> = T extends DDB<infer S, infer F>
  ? Item<F, S[typeof DDBKey]>
  : never

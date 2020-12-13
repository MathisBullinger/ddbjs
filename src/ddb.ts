import * as AWS from 'aws-sdk'

export type Schema<T extends Fields> = T & { key: Key<T> }
export type Fields = Record<string, SchemaValueType>
type SchemaValueType = StringConstructor | NumberConstructor
export type Key<T extends Fields> = keyof T | CompositeKey<T>
export type CompositeKey<T extends Fields> = [hash: keyof T, sort: keyof T]
type SchemaValue<T extends SchemaValueType> = T extends StringConstructor
  ? string
  : number

type KeyValue<
  T extends Schema<F>,
  F extends Fields = Omit<T, 'key'>
> = T['key'] extends CompositeKey<F>
  ? [SchemaValue<F[T['key'][0]]>, SchemaValue<F[T['key'][1]]>]
  : T['key'] extends keyof F
  ? [SchemaValue<F[T['key']]>]
  : never

export class DDB<T extends Schema<F>, F extends Fields = Omit<T, 'key'>> {
  private readonly client: AWS.DynamoDB.DocumentClient

  constructor(public readonly table: string, private readonly schema: T) {
    this.client = new AWS.DynamoDB.DocumentClient()
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

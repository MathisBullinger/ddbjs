type Schema<T extends Fields> = T & { key: Key<T> }

type Fields = Record<string, SchemaValueType>

type SchemaValueType = StringConstructor | NumberConstructor

type Key<T extends Fields> = keyof T | CompositeKey<T>

type CompositeKey<T extends Fields> = [hash: keyof T, sort: keyof T]

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

type FlatKeyValue<
  T extends Schema<F>,
  F extends Fields = Omit<T, 'key'>
> = T extends CompositeKey<F> ? KeyValue<T, F> : KeyValue<T, F>[0]

type KeyFields<T extends Fields, K extends Key<T>> = keyof Pick<
  T,
  K extends CompositeKey<T> ? K[0] | K[1] : K
>

type Item<TFields extends Fields, TKey extends Key<TFields>> = {
  [K in KeyFields<TFields, TKey>]: SchemaValue<TFields[K]>
} &
  {
    [K in keyof Omit<TFields, KeyFields<TFields, TKey>>]?: SchemaValue<
      TFields[K]
    >
  }

type AtLeastOne<T, Keys extends keyof T = keyof T> = Pick<
  T,
  Exclude<keyof T, Keys>
> &
  {
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>
  }[Keys]

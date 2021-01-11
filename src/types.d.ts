type Schema<T extends Fields> = T & Readonly<{ key: Key<T> }>

type Fields = Readonly<Record<string, SchemaValueType>>

type PrimitiveConstructor = StringConstructor | NumberConstructor

type SchemaValueType =
  | PrimitiveConstructor
  | [StringConstructor]
  | [NumberConstructor]
  | []
  | { [K: string]: SchemaValueType }

type PrimitiveKeys<T extends Fields> = {
  [K in keyof T]: T[K] extends Function ? K : never
}[keyof T]
type PrimitiveFields<T extends Fields> = Pick<T, PrimitiveKeys<T>>

type Key<T extends Fields> = keyof PrimitiveFields<T> | CompositeKey<T>

type CompositeKey<R extends Fields, T extends Fields = PrimitiveFields<R>> = [
  hash: keyof T,
  sort: keyof T
]

type SchemaValue<T extends SchemaValueType> = T extends PrimitiveConstructor
  ? PrimitiveConstructorType<T>
  : T extends []
  ? any[]
  : T extends any[]
  ? Readonly<NonEmpty<PrimitiveConstructorType<T[number]>>>
  : T extends Fields
  ? Item<T, never>
  : never

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
> = T['key'] extends CompositeKey<F> ? KeyValue<T, F> : KeyValue<T, F>[0]

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

type DBItem<T extends Fields> = { [K in keyof T]: SchemaValue<T[K]> } &
  Record<string, any>

type UpdateInput<
  T extends Schema<F>,
  F extends Fields = Omit<T, 'key'>,
  TI = Omit<Item<F, T['key']>, KeyFields<F, T['key']>>
> = {
  set?: Record<string, any> & TI
  remove?: string[]
}

type ItemUpdate<
  T extends Schema<F>,
  F extends Fields = Omit<T, 'key'>,
  U extends UpdateInput<T, F> = UpdateInput<T, F>
> = NonNullable<U['set']> & { $remove?: U['remove'] }

// generic helper types

type NonEmpty<T> = [T, ...T[]]

type AtLeastOne<T, Keys extends keyof T = keyof T> = Pick<
  T,
  Exclude<keyof T, Keys>
> &
  {
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>
  }[Keys]

type NotEmptyObj<T extends Record<string, any>> = keyof T extends never
  ? never
  : T

type PrimitiveConstructorType<
  T extends PrimitiveConstructor
> = T extends StringConstructor ? string : number

type ExplTypes<T extends Record<string, any>> = {
  [K in keyof T]?: 'Set' | 'List'
} &
  Record<string, 'Set' | 'List'>

import { DDBKey } from './ddb'
export type KeySym = typeof DDBKey

export type Schema<T extends Fields> = T & Readonly<{ [DDBKey]: Key<T> }>

export type Fields = Readonly<Record<string, SchemaValueType>>

export type PrimitiveConstructor =
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor

export type SchemaValueType =
  | PrimitiveConstructor
  | [StringConstructor]
  | [NumberConstructor]
  | []
  | { [K: string]: SchemaValueType }
  | ObjectConstructor

export type PrimitiveKeys<T extends Fields> = {
  [K in keyof T]: T[K] extends Function ? K : never
}[keyof T]
export type PrimitiveFields<T extends Fields> = Pick<T, PrimitiveKeys<T>>

export type Key<T extends Fields> = keyof PrimitiveFields<T> | CompositeKey<T>

export type CompositeKey<
  R extends Fields,
  T extends Fields = PrimitiveFields<R>
> = [hash: keyof T, sort: keyof T]

export type SchemaValue<T extends SchemaValueType> =
  T extends PrimitiveConstructor
    ? PrimitiveConstructorType<T>
    : T extends []
    ? any[]
    : T extends any[]
    ? PrimitiveConstructorType<T[number]>[]
    : T extends ObjectConstructor
    ? any
    : T extends Fields
    ? Item<T, never>
    : never

export type KeyValue<
  T extends Schema<F>,
  F extends Fields = Omit<T, KeySym>
> = T[KeySym] extends CompositeKey<F>
  ? [SchemaValue<F[T[KeySym][0]]>, SchemaValue<F[T[KeySym][1]]>]
  : T[KeySym] extends keyof F
  ? [SchemaValue<F[T[KeySym]]>]
  : never

export type FlatKeyValue<
  T extends Schema<F>,
  F extends Fields = Omit<T, KeySym>
> = T[KeySym] extends CompositeKey<F> ? KeyValue<T, F> : KeyValue<T, F>[0]

export type KeyFields<T extends Fields, K extends Key<T>> = keyof Pick<
  T,
  K extends CompositeKey<T> ? K[0] | K[1] : K
>

export type Item<TFields extends Fields, TKey extends Key<TFields>> = {
  [K in KeyFields<TFields, TKey>]: SchemaValue<TFields[K]>
} &
  {
    [K in keyof Omit<TFields, KeyFields<TFields, TKey>>]?: SchemaValue<
      TFields[K]
    >
  }

export type DBItem<T extends Fields> = { [K in keyof T]: SchemaValue<T[K]> } &
  Record<string, any>

export type UpdateInput<
  T extends Schema<F>,
  F extends Fields = Omit<T, KeySym>
> = {
  set?: UpdateMap<F>
  remove?: string[]
  add?: Record<string, string[] | number[] | number>
  delete?: Record<string, string[] | number[]>
}

type UpdateMap<T extends Fields> = Partial<
  {
    [K in keyof T]?: { [K_ in K]: SchemaValue<T[K]> } &
      (T[K] extends Fields ? MapPrefix<UpdateMap<T[K]>, K & string> : {})
  }[keyof T]
>

type MapPrefix<T, P extends string> = {
  [K in keyof T as `${P}.${K & string}`]: T[K]
}

export type ItemUpdate<
  T extends Schema<F>,
  F extends Fields = Omit<T, KeySym>,
  U extends UpdateInput<T, F> = UpdateInput<T, F>
> = NonNullable<U['set']> & {
  $remove?: U['remove']
  $add?: U['add']
  $delete?: U['delete']
}

export type KeyPath<T extends Fields> =
  | keyof T
  | {
      [K in keyof T]: K extends string
        ? T[K] extends []
          ? `${K}[${number}]${string}`
          : T[K] extends any[]
          ? `${K}[${number}]`
          : T[K] extends ObjectConstructor | Record<string, SchemaValueType>
          ? `${K}${'[' | '.'}${string}`
          : never
        : never
    }[keyof T]

// generic helper types

export type AtLeastOne<T, Keys extends keyof T = keyof T> = Pick<
  T,
  Exclude<keyof T, Keys>
> &
  {
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>
  }[Keys]

export type NotEmptyObj<T extends Record<string, any>> = keyof T extends never
  ? never
  : T

export type PrimitiveConstructorType<T extends PrimitiveConstructor> =
  T extends StringConstructor
    ? string
    : T extends NumberConstructor
    ? number
    : boolean

export type ExplTypes<T extends Record<string, any>> = {
  [K in keyof T]?: 'Set' | 'List'
} &
  Record<string, 'Set' | 'List'>

export type AttributeType =
  | 'S'
  | 'SS'
  | 'N'
  | 'NS'
  | 'B'
  | 'BS'
  | 'BOOL'
  | 'NULL'
  | 'L'
  | 'M'

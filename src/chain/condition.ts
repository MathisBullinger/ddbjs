import BaseChain from './base'
import type { Fields, AttributeType } from '../types'
import * as expr from '../expression'
import * as naming from '../utils/naming'
import partial from 'snatchblock/partial'
import type { λ } from 'snatchblock/types'

type Comparator = '=' | '<>' | '<' | '<=' | '>' | '>='

type CompArgs<T, U> =
  | [field: keyof T & string, comparator: Comparator, value: any]
  | [cb: (chain: U) => U]

const isCB = <T, U>(args: CompArgs<T, U>): args is [(c: U) => U] =>
  typeof args[0] === 'function'

type AddCond<T, F extends Fields> = ((
  ...args: CompArgs<F, ConditionChain<T, F>>
) => ConditionChain<T, F>) & { not: AddCond<T, F> } & MapReturn<
    ConditionChain<T, F>['functions_'],
    ConditionChain<T, F>
  >

type MapReturn<T extends Record<string, λ>, R> = {
  [K in keyof T]: λ<Parameters<T[K]>, R>
}

export default abstract class ConditionChain<
  T,
  F extends Fields
> extends BaseChain<T, F> {
  private ifAndOr = (
    con: 'AND' | 'OR',
    wrap: ConditionWrapper
  ): AddCond<T, F> => {
    const fun = (...args: CompArgs<F, ConditionChain<T, F>>) => {
      if (!isCB(args)) {
        this.addCondition(wrap(new Comparison(...args)), con)
        return this
      } else {
        const cond = this.cloneConditon()
        delete this.condition
        const chain = args[0](this)
        chain.condition = wrap(chain.condition!)
        chain.addCondition(cond, con, true)
        return chain as this
      }
    }

    Reflect.defineProperty(fun, 'not', {
      get: () => this.makeNegated(this.condMeths.get(fun)!),
    })
    this.functions.forEach((f: any, k) => {
      ;(fun as any)[k] = (...args: any[]) => {
        this.addCondition(f(...args), con)
        return this
      }
    })
    return fun as any
  }

  private condMeths = new Map<
    (...args: any[]) => any,
    (wrap: ConditionWrapper) => λ
  >()
  private conditionFactory =
    <T extends λ<[ConditionWrapper], λ>>(handler: T) =>
    (wrapper?: ConditionWrapper): ReturnType<T> => {
      const wrapped = handler(wrapper ?? (v => v))
      this.condMeths.set(wrapped, handler)
      return wrapped as any
    }

  private functions_ = {
    attributeExists: (path: keyof F) =>
      new Function('attribute_exists', path as string),
    attributeNotExists: (path: keyof F) =>
      new Function('attribute_not_exists', path as string),
    attributeType: (path: keyof F, type: AttributeType) =>
      new Function('attribute_type', path as string, type),
    beginsWith: (path: keyof F, substr: string) =>
      new Function('begins_with', path as string, substr),
    contains: (path: keyof F, operand: unknown) =>
      new Function('contains', path as string, operand),
  }
  private functions = new Map(Object.entries(this.functions_))

  public if = this.conditionFactory(partial(this.ifAndOr, 'AND'))()
  public andIf = this.conditionFactory(partial(this.ifAndOr, 'AND'))()
  public orIf = this.conditionFactory(partial(this.ifAndOr, 'OR'))()

  private addCondition(
    cond: ConditionList | undefined,
    con: 'AND' | 'OR',
    left = false
  ) {
    if (!cond) return
    if (!this.condition) this.condition = cond
    else
      this.condition = left
        ? [cond, con, this.condition]
        : [this.condition, con, cond]
  }

  protected condition?: ConditionList

  protected buildCondition(): expr.ConditionExpression | undefined {
    if (!this.condition) return

    const ExpressionAttributeNames: Record<string, any> = {}
    const ExpressionAttributeValues: Record<string, any> = {}

    const ConditionExpression = this.serialize(
      this.condition,
      name => {
        if (naming.valid(name)) return name
        const key = `#cn_${Object.keys(ExpressionAttributeNames).length}`
        ExpressionAttributeNames[key] = name
        return key
      },
      value => {
        const key = `:cv_${Object.keys(ExpressionAttributeValues).length}`
        ExpressionAttributeValues[key] = value
        return key
      }
    )

    return {
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      ConditionExpression,
    }
  }

  private serialize(
    cond: ConditionList,
    name: (key: string) => string,
    value: (value: unknown) => string
  ): string {
    if (cond instanceof Condition) return cond.expr(name, value)
    return [cond[0], cond[2]]
      .map(v => `(${this.serialize(v, name, value)})`)
      .join(` ${cond[1]} `)
  }

  public cloneConditon(node = this.condition): ConditionList | undefined {
    if (!node) return
    if (node instanceof Condition) return node
    return [this.cloneConditon(node[0])!, node[1], this.cloneConditon(node[2])!]
  }

  private makeNegated(
    f: (wrap: ConditionWrapper) => λ,
    wrap: ConditionWrapper = v => v
  ) {
    const negate = (v: any) => wrap(new Negated(v, this.serialize.bind(this)))
    const negated = (...args: CompArgs<F, ConditionChain<T, F>>) => {
      return this.conditionFactory(f)(negate)(...args)
    }
    Reflect.defineProperty(negated, 'not', {
      get: () => this.makeNegated(f, negate),
    })
    this.functions.forEach((_, k) => {
      ;(negated as any)[k] = (...args: any[]) =>
        negated((v: any) => v.if[k](...args))
    })
    return negated
  }
}

type ConditionList = Condition | [ConditionList, 'AND' | 'OR', ConditionList]
type ConditionWrapper = (condition: ConditionList) => ConditionList

abstract class Condition {
  abstract expr(
    name: (key: string) => string,
    value: (value: unknown) => string
  ): string
}

class Comparison extends Condition {
  constructor(
    private readonly a: string,
    private readonly comp: Comparator,
    private readonly b: unknown
  ) {
    super()
  }

  expr(name: (key: string) => string, value: (value: unknown) => string) {
    return `${name(this.a)} ${this.comp} ${value(this.b)}`
  }
}

class Negated extends Condition {
  constructor(
    private readonly condition: ConditionList,
    private readonly serialize: (
      condition: ConditionList,
      name: (key: string) => string,
      value: (value: unknown) => string
    ) => string
  ) {
    super()
  }

  expr(name: (key: string) => string, value: (value: unknown) => string) {
    return `NOT (${this.serialize(this.condition, name, value)})`
  }
}

class Function extends Condition {
  static none = Symbol('none')

  constructor(
    private readonly name: string,
    private readonly path: string,
    private readonly arg: unknown = Function.none
  ) {
    super()
  }

  expr(name: (key: string) => string, value: (value: unknown) => string) {
    const args = [name(this.path)]
    if (this.arg !== Function.none) args.push(value(this.arg))
    return `${this.name}(${args.join(',')})`
  }
}

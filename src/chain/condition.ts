import BaseChain, { Config, UtilFlags } from './base'
import * as expr from '../expression'
import partial from 'snatchblock/partial'
import oneOf from 'snatchblock/oneOf'
import type { Fields, AttributeType, KeyPath, ScFields, Schema } from '../types'
import type { λ } from 'snatchblock/types'
import { capitalize, uncapitalize } from 'snatchblock/string'

type CondArgs<T extends Fields, U> =
  | [a: Operand<T>, comparator: Comparator, b: Operand<T>]
  | [Operand<T>, 'between', Operand<T>, Operand<T>]
  | [Operand<T>, 'in', ...Operand<T>[]]
  | [cb: (chain: U) => U]

type Comparator = '=' | '<>' | '<' | '<=' | '>' | '>='

type Operand<T extends Fields> = Operand_<T> | { size: Operand_<T> }

type Operand_<T extends Fields> =
  | KeyPath<T>
  | Literal
  | { literal: Literal }
  | { path: string }

type Literal = string | number | boolean | null

const isCB = <T extends Fields, U>(
  args: CondArgs<T, U>
): args is [(c: U) => U] => typeof args[0] === 'function'

const isComp = <T extends Fields, U>(
  args: CondArgs<T, U>
): args is [a: Operand<T>, comparator: Comparator, b: Operand<T>] =>
  args.length === 3

type AddCond<R, T extends CondConf<any, any>, U> = ((
  ...args: CondArgs<F<T>, ConditionChain<R, T, U>>
) => ConditionChain<R, T, U>) & { not: AddCond<R, T, U> } & MapReturn<
    ConditionChain_<R, T, U>['functions_'],
    ConditionChain<R, T, U>
  >

type MapReturn<T extends Record<string, λ>, R> = {
  [K in keyof T]: λ<Parameters<T[K]>, R>
}

type F<T extends Config<any>> = ScFields<T['schema']>

type CondConf<T extends Schema<any>, V extends 'if' | 'filter'> = Config<T> & {
  verb: V
}

type CondExpr<T extends CondConf<any, any>> = T extends CondConf<any, infer V>
  ? V extends 'if'
    ? expr.ConditionExpression
    : V extends 'filter'
    ? expr.FilterExpression
    : never
  : never

abstract class ConditionChain_<
  R,
  C extends CondConf<any, any>,
  U extends UtilFlags
> extends BaseChain<R, C, U> {
  constructor(config: C, flags: U) {
    super(config, flags)
    this.onCloneHooks.push(chain => {
      chain.condition = this.cloneConditon()
    })

    Object.assign(
      this,
      Object.fromEntries(
        ['', 'and', 'or'].map(c => [
          uncapitalize(c + capitalize(config.verb)),
          this.conditionFactory(
            partial(this.ifAndOr, c.toUpperCase() || 'AND')
          )(),
        ])
      )
    )
  }

  private resolveOperand(op: Operand<F<C>>): string | Function {
    if (typeof op === 'object' && op !== null) {
      const key = Object.keys(op)[0]
      if (
        Object.keys(op).length !== 1 ||
        !oneOf(key, 'size', 'literal', 'path')
      )
        throw Error('operand must have form {[path|literal|size]:...}')

      if (key === 'path') return this.name((op as any).path)
      if (key === 'literal') return this.value((op as any).literal)
      if (key === 'size')
        return new Function(
          'size',
          this.resolveOperand((op as any).size) as string
        )
    }

    if (typeof op === 'string' && op.split(/[\.\[]/)[0] in this.config.schema)
      return this.name(op)

    return this.value(op)
  }

  private ifAndOr = (
    con: 'AND' | 'OR',
    wrap: ConditionWrapper
  ): AddCond<R, C, U> => {
    const fun = (...args: CondArgs<F<C>, ConditionChain<R, C, U>>) => {
      if (isCB(args)) {
        const cond = this.cloneConditon()
        delete this.condition
        const chain = args[0](this as any)
        chain.condition = wrap(chain.condition!)
        chain.addCondition(cond, con, true)
        return chain as ConditionChain<R, C, U>
      }
      if (args[1] === 'between') {
        const [a, _, b, c] = args
        this.addCondition(
          wrap(
            new Between(
              ...([a, b, c].map(v => this.resolveOperand(v)) as [any, any, any])
            )
          ),
          con
        )
        return this
      }
      if (args[1] === 'in') {
        const [v, _, ...list] = args
        this.addCondition(
          wrap(
            new In(
              this.resolveOperand(v),
              list.map(e => this.resolveOperand(e))
            )
          ),
          con
        )
        return this
      }
      if (isComp(args)) {
        const [a, comp, b] = args
        this.addCondition(
          wrap(
            new Comparison(this.resolveOperand(a), comp, this.resolveOperand(b))
          ),
          con
        )
        return this
      }
      throw Error('unknown condition format')
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

  protected functions_ = {
    attributeExists: (path: KeyPath<F<C>> & string) =>
      new Function('attribute_exists', this.name(path)),
    attributeNotExists: (path: KeyPath<F<C>> & string) =>
      new Function('attribute_not_exists', this.name(path)),
    attributeType: (path: KeyPath<F<C>> & string, type: AttributeType) =>
      new Function('attribute_type', this.name(path), this.value(type)),
    beginsWith: (path: KeyPath<F<C>> & string, substr: string) =>
      new Function('begins_with', this.name(path), this.value(substr)),
    contains: (path: KeyPath<F<C>> & string, operand: unknown) =>
      new Function('contains', this.name(path), this.value(operand)),
  }
  private functions = new Map(Object.entries(this.functions_))

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

  protected buildCondition(): CondExpr<C> | undefined {
    if (this.condition) {
      return {
        ExpressionAttributeNames: Object.fromEntries(this.attrNames.key),
        ExpressionAttributeValues: Object.fromEntries(this.attrValues.key),
        [this.condName]: this.serialize(this.condition),
      } as any
    }
  }

  private get condName() {
    if (this.config.verb === 'if') return 'ConditionExpression'
    if (this.config.verb === 'filter') return 'FilterExpression'
    throw Error(`unknown condition verb: ${JSON.stringify(this.config.verb)}`)
  }

  private serialize(cond: ConditionList): string {
    if (cond instanceof Condition) return cond.expr()
    return [cond[0], cond[2]]
      .map(v => `(${this.serialize(v)})`)
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
    const negated = (...args: CondArgs<F<C>, ConditionChain<R, C, U>>) => {
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
  abstract expr(): string
}

class Comparison extends Condition {
  constructor(
    private readonly a: Literal | Condition,
    private readonly comp: Comparator,
    private readonly b: Literal | Condition
  ) {
    super()
  }

  expr() {
    const [a, b] = [this.a, this.b].map(v =>
      v instanceof Condition ? v.expr() : v
    )
    return `${a} ${this.comp} ${b}`
  }
}

class Between extends Condition {
  constructor(
    private readonly op: Literal | Condition,
    private readonly a: Literal | Condition,
    private readonly b: Literal | Condition
  ) {
    super()
  }

  expr() {
    const [v, a, b] = [this.op, this.a, this.b].map(v =>
      v instanceof Condition ? v.expr() : v
    )
    return `${v} BETWEEN ${a} AND ${b}`
  }
}

class In extends Condition {
  constructor(
    private readonly op: Literal | Condition,
    private readonly list: (Literal | Condition)[]
  ) {
    super()
  }

  expr() {
    const [v, ...list] = [this.op, ...this.list].map(v =>
      v instanceof Condition ? v.expr() : v
    )
    return `${v} IN (${list.join(',')})`
  }
}

class Negated extends Condition {
  constructor(
    private readonly condition: ConditionList,
    private readonly serialize: (condition: ConditionList) => string
  ) {
    super()
  }

  expr() {
    return `NOT (${this.serialize(this.condition)})`
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

  expr() {
    const args: any[] = [this.path]
    if (this.arg !== Function.none) args.push(this.arg)
    return `${this.name}(${args.join(',')})`
  }
}

type ConditionChain<
  R,
  C extends CondConf<any, any>,
  U extends UtilFlags
> = ConditionChain_<R, C, U> &
  (C extends CondConf<any, infer V>
    ? { [K in V | `${'and' | 'or'}${Capitalize<V>}`]: AddCond<R, C, U> }
    : never)

export default ConditionChain_ as unknown as new <
  R,
  C extends CondConf<any, any>,
  U extends UtilFlags
>(
  config: C,
  flags: U
) => ConditionChain<R, C, U>

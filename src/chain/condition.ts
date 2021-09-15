import BaseChain from './base'
import * as expr from '../expression'
import partial from 'snatchblock/partial'
import oneOf from 'snatchblock/oneOf'
import type { Fields, AttributeType, KeyPath } from '../types'
import type { λ } from 'snatchblock/types'

type CondArgs<T extends Fields, U> =
  | [a: Operand<T>, comparator: Comparator, b: Operand<T>]
  | [Operand<T>, 'between', Operand<T>, Operand<T>]
  | [Operand<T>, 'in', ...Operand<T>[]]
  | [cb: (chain: U) => U]

type Comparator = '=' | '<>' | '<' | '<=' | '>' | '>='

type Operand<T extends Fields> =
  | KeyPath<T>
  | Literal
  | { size: KeyPath<T> }
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

type AddCond<T, F extends Fields> = ((
  ...args: CondArgs<F, ConditionChain<T, F>>
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
  private resolveOperand(op: Operand<F>) {
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
        return new Function('size', this.name((op as any).size))
    }

    if (typeof op === 'string' && op.split(/[\.\[]/)[0] in this.fields)
      return this.name(op)
    return this.value(op)
  }

  private ifAndOr = (
    con: 'AND' | 'OR',
    wrap: ConditionWrapper
  ): AddCond<T, F> => {
    const fun = (...args: CondArgs<F, ConditionChain<T, F>>) => {
      if (isCB(args)) {
        const cond = this.cloneConditon()
        delete this.condition
        const chain = args[0](this)
        chain.condition = wrap(chain.condition!)
        chain.addCondition(cond, con, true)
        return chain as this
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
    attributeExists: (path: KeyPath<F> & string) =>
      new Function('attribute_exists', this.name(path)),
    attributeNotExists: (path: KeyPath<F> & string) =>
      new Function('attribute_not_exists', this.name(path)),
    attributeType: (path: KeyPath<F> & string, type: AttributeType) =>
      new Function('attribute_type', this.name(path), this.value(type)),
    beginsWith: (path: KeyPath<F> & string, substr: string) =>
      new Function('begins_with', this.name(path), this.value(substr)),
    contains: (path: KeyPath<F> & string, operand: unknown) =>
      new Function('contains', this.name(path), this.value(operand)),
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
    if (this.condition) {
      return {
        ExpressionAttributeNames: Object.fromEntries(this.attrNames.key),
        ExpressionAttributeValues: Object.fromEntries(this.attrValues.key),
        ConditionExpression: this.serialize(this.condition),
      }
    }
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
    const negated = (...args: CondArgs<F, ConditionChain<T, F>>) => {
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

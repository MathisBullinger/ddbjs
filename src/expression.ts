import * as naming from './utils/naming'

type Expression = {
  UpdateExpression: string
  ExpressionAttributeValues?: Record<string, any>
  ExpressionAttributeNames?: Record<string, string>
}

const build = (expr: Expression): Expression => {
  const result = { ...expr }
  Object.entries(result).forEach(([k, v]) => {
    if (typeof v === 'object' && Object.keys(v).length === 0)
      delete result[k as keyof Expression]
  })
  return result
}

export const set = (input?: Record<string, any>): Expression | undefined => {
  if (!input || !Object.keys(input).length) return

  const pairs: [string, string][] = []
  const ExpressionAttributeValues: Expression['ExpressionAttributeValues'] = {}
  const ExpressionAttributeNames: Expression['ExpressionAttributeNames'] = {}

  const entries = Object.entries(input)

  for (let i = 0; i < entries.length; i++) {
    const [key, value] = entries[i]
    const av = `:s${i}`
    let name = key
    if (!naming.valid(name)) {
      name = `#s${Object.keys(ExpressionAttributeNames).length}`
      ExpressionAttributeNames[name] = key
    }
    pairs.push([name, av])
    ExpressionAttributeValues[av] = value
  }

  return build({
    UpdateExpression: 'SET ' + pairs.map(v => v.join('=')).join(', '),
    ExpressionAttributeValues,
    ExpressionAttributeNames,
  })
}

export const remove = (...fields: string[]): Expression | undefined => {
  fields = Array.from(new Set(fields))
  if (!fields.length) return
  const names: string[] = []
  const ExpressionAttributeNames: Expression['ExpressionAttributeNames'] = {}

  for (const field of fields) {
    if (naming.valid(field)) names.push(field)
    else {
      const name = `#r${Object.keys(ExpressionAttributeNames).length}`
      names.push(name)
      ExpressionAttributeNames[name] = field
    }
  }

  return build({
    UpdateExpression: 'REMOVE ' + names.join(', '),
    ExpressionAttributeNames,
  })
}

export const merge = (
  ...expressions: (Expression | undefined)[]
): Expression => {
  const exprs: string[] = []
  const ExpressionAttributeValues: Expression['ExpressionAttributeValues'] = {}
  const ExpressionAttributeNames: Expression['ExpressionAttributeNames'] = {}

  for (const e of expressions) {
    if (!e) continue
    exprs.push(e.UpdateExpression)

    for (const [k, v] of Object.entries(e.ExpressionAttributeValues ?? {})) {
      if (k in ExpressionAttributeValues)
        throw Error(`duplicate key '${k}' in ExpressionAttributeValues`)
      ExpressionAttributeValues[k] = v
    }

    for (const [k, v] of Object.entries(e.ExpressionAttributeNames ?? {})) {
      if (k in ExpressionAttributeNames)
        throw Error(`duplicate key '${k} in ExpressionAttributeNames`)
      ExpressionAttributeNames[k] = v
    }
  }

  const expr: Expression = {
    UpdateExpression: exprs.join(' '),
  }
  if (Object.keys(ExpressionAttributeValues).length)
    expr.ExpressionAttributeValues = ExpressionAttributeValues
  if (Object.keys(ExpressionAttributeNames).length)
    expr.ExpressionAttributeNames = ExpressionAttributeNames
  return expr
}

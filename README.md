# DynamoDB with a usable API.

This library provides a friendlier API for DynamoDB.

# Installation

```sh
npm install idb
```

Then, using a bundler like Webpack or Rollup, import as:

```js
import { DDB } from 'ddbjs'

const db = new DDB(…)
```

# API

## DDB Constructor

```js
const db = new DDB(name, schema, params)
```

- `name`: Name of DynamoDB table.
- `schema`: The "schema" of the database. This must include which field(s) are used as key and their respective types. See [schema](#schema) for more information.
- `params` (optional): [Parameters](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#constructor-property) passed to DynamoDB document client.

## Schema

In the schema you can define which attributes exist and what their types are. This is required for the attributes that are used as key, but optional for everything else (since DynamoDB does not have a fixed schema, you can of course always pass attributes that aren't declared in the schema). The schema you pass in is also used to generate static types in TypeScript. You must specify which attribute(s) are used as hash and partition key in the `[DDBKey]` field (`[DDBKey]` is a symbol exportet from the library, you can also access it as the static `DDB.key` property). If the `[DDBKey]` is a string, it is used as hash key. If you use a sort key you must declare them like this `[DDBKey]: ['<hash>', '<sort>']`.

The [DynamoDB data types](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html) are declared as following:

| type    | notation               |
| ------- | ---------------------- |
| Number  | `Number`               |
| String  | `String`               |
| Boolean | `Boolean`              |
| Set     | `[String]`, `[Number]` |
| List    | `[]`                   |
| Map     | `{}`                   |

If you use sets, you must declare them as such in the schema to be able to insert or update them as a regular array. Otherwise, the library will assume that the array you're inserting is a list, or you have to explicitly `cast` it to a set for that operation.

### Casting List <-> Set

If you want to insert/update an attribute as a set that hasn't been declared in the schema as such or insert/update an attribute as a list that has been declared a set in the schema, you can chain `.cast()` after the `.put()` and `.update()` methods to override its type:

```js
db.update('id', { someSet: ['a', 'b'] }).cast({ someSet: 'Set' })
```

### Examples

_A users table that uses the `id` attribute as partition key:_

```js
{
  [DDB.Key]: 'id',
  id: String,
  age: Number,
  tags: [Number], // a set of numbers
  messages: [], // a list
}
```

_A table that uses key (`pk` as partition key and `sk` as sort key):_

```js
{
  [DDB.Key]: ['pk', 'sk'],
  pk: String,
  sk: Number,
}
```

## `get`

Gets an item by its key.

```js
const user = await db.get(123)

// with a composite key
const data = await db.get('foo', 'bar') // hash: 'foo', sort: 'bar'
```

## `put`

Writes an item to the database. The key attributes must be specified.

```js
await db.put({ id: 'foo', name: 'john' })

// The inserted item or (in case of overwrite) old item can be returned
// by chaining `.returning()`. Valid arguments are 'NEW' and 'OLD'.
const oldUser = await db.put({ id: 'foo', name: 'jane' }).returning('OLD')
console.log(oldUser) //> { id: 'foo', name: 'john' }
```

## `delete`

Deletes an item from the database by its key.

```js
await db.delete('foo')

// delete with composite key
await db.delete('hash', 'sort')

// return deleted item
await db.put({ id: 'foo', name: 'john' })
const user = await db.delete('foo').returning('OLD')
console.log(user) //> { id: 'foo', name: 'john' }
```

## `update`

Update an existing item. By default, if the key does not yet exist, it writes a new item to the database. You can not include the key parameters in the updated fields.

```js
await db.update('asdf', { name: 'foo' })

// Chain `.returning()` to receive the old or updated item.
// Valid arguments are 'NEW', 'OLD', 'UPDATED_NEW', and 'UPDATED_OLD'.
const oldName = await db.update('asdf', { name: 'bar' }).returning('UPDATED_OLD')
console.log(oldName) //> { name: 'foo' }

// Chain `.ifExists()` to prevent creating a new item if the key doesn't exist.
await db.update('new_key', { name: 'foo' }).ifExists() // throws error

// with composite key
await db.update(['hash', 'sort'], { data: '…' })
```

## Condition Expressions

The `put`, `update`, and `delete` operations can all include [condition expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html).
A condition is added by chaining `.if( [condition] )`, `andIf`, or `orIf` (`if`
has the same behavior as `andIf`).

### Comparisons

`.if` accepts the arguments `operand, comparator, operand` where `comparator` is
one of `=`, `<>`, `<`, `<=`, `>`, `>=` and `operand` is interpreted like this:

If `operand` is a key specified in the schema, it will refer to that key, otherwise
it is interpreted as a literal value. This can be overridden be specifying `operator` as
`{ path: ... }`, or `{ literal: ... }`.

I.e. `.if({ path: 'foo' }, '=', { literal: 'bar' })` will result in the DynamoDB
expression including

```js
{ 
  ConditionExpression: 'foo = :cv_0',
  ExpressionAttributeValues: { ':cv_0': 'bar' }
}
```

whereas `.if({ path: 'foo' }, '=', { path: 'bar' })` will be translated to

```js
{
  ConditionExpression: 'foo = bar'
}
```

which is checking if the value at `foo` equals the value at `bar`.

Assuming the schema declared a key `foo` and doesn't include a key `bar`, calling
`.if('foo', '=', 'bar')` will result in the former expression.

Other than the listed binary comparisons, `if` also accepts the arguments
`if(a, 'between', b, c)` which checks if `b ≤ a ≤ c`, and `if(a, 'in', ...list)`
which checks if `a` matches any of the operands in `list`.

### Negating conditions

All conditions can be negated by chaining `.not` after `.if`, `.andIf`, or `.orIf`.
E.g. `.if.not(foo, '<', bar)` will result in the condition expression `NOT (foo < bar)`, which is functionally equivalent to `foo >= bar`.

`.not` can also be chained to itself to negate a condition multiple times.

### Functions

All the DynamoDB condition functions can be accessed as `.if.[function](...args)`.

The available functions are:

Function | Description
---|---
`attributeExists(path)` | True if the item contains the attribute specified by `path`.
`attributeNotExits(path)` | True if the attribute specified by `path` does not exist in the item.
`attributeType(path, type)` | True if the attribute at `path` is of `type`. <br>`type` can be any of:<br><ul><li>`'S'` - String</li><li>`'SS'` - String Set</li><li>`'N'` - Number</li><li>`'NS'` - Number Set</li><li>`'B'` - Binary</li><li>`'BS'` - Binary Set</li><li>`'BOOL'` - Boolean</li><li>`'NULL'` - Null</li><li>`'L'` - List</li><li>`'M'` - Map</li></ul>
`beginsWith(path, substr)` | True if the attribute specified by `path` begins with `substr`.
`contains(path, operand)` | True if attribute at `path` is a string or set containing `operand`.

The `size` function can be used in any operand by specifying the operand as
`{ size: <path> }`, i.e. `if({size: 'foo'}, '<', 5)` will check if the size of
the attribute at path `'foo'` is smaller than 5, and `if({size: 'listA'}, '>', {size: 'listB'})` might be used to check if `listA` has more elements than `listB`.


### Grouping

The `if`, `orIf`, `andIf`, and `not` methods can all be alternatively invoked by 
passing a callback that takes the current chain as its first argument. All 
conditions specified inside the callback will be grouped together.

```js
// (foo AND bar) OR baz
.if(foo).andIf(bar).orIf(baz)

// foo AND (bar OR baz)
.if(foo).andIf(chain => chain.if(bar).orIf(baz))
```



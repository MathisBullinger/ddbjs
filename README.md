# DynamoDB with a usable API.

This library provides a friendlier API for DynamoDB.

# Installation

```sh
npm install idb
```

Then, using a bundler like Webpack or Rollup, import like this:

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
- `params` (optional): Parameters passed to DynamoDB document client.
  E.g.:
  ```js
  { region: 'localhost', endpoint: 'http://localhost:4567' }
  ```

## Schema

In the schema you can define which attributes exist and what their types are. This is required for the attributes that are used as key, but optional for everything else (since DynamoDB does not have a fixed schema, you can of course always pass attributes that aren't declared in the schema). The schema you pass in is also used to generate static types in TypeScript. You must specify which attribute(s) are used as hash and partition key in the `key` field. If the `key` is a string, it is used as hash key. If you use a sort key you must declare them like this `key: ['<hash>', '<sort>']`.

The [DynamoDB data types](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html) are declared as following:

| type    | notation               |
| ------- | ---------------------- |
| Number  | `Number`               |
| String  | `String`               |
| Boolean | `Boolean`              |
| Set     | `[String]`, `[Number]` |
| List    | `[]`                   |
| Map     | `{}`                   |

You must declare sets in the schema, otherwise when you insert an array it is assumed to be a list.

### Casting List <-> Set

If you want to insert/update an attribute as a set that hasn't been declared in the schema as such or insert/update an attribute as a list that has been declared a set in the schema, you can chain `.cast()` after the `.put()` and `.update()` methods to override its type:

```js
db.update('id', { someSet: ['a', 'b'] }).cast({ someSet: 'Set' })
```

### Examples

_A users table that uses the `id` attribute as partition key:_

```js
{
  key: 'id',
  id: String,
  age: Number,
  tags: [Number], // a set of numbers
  messages: [], // a list
}
```

_A table that uses key (`pk` as partition key and `sk` as sort key`):_

```js
{
  key: ['pk', 'sk'],
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

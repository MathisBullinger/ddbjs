# DynamoDB with a usable API.

This library provides a friendlier API for DynamoDB.

# Installation

```sh
npm install idb
```

Then import like this:

```
import { DDB } from 'sane-ddb'
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
const user = await ddb.get(123)

// with a composite key
const data = await ddb.get('foo', 'bar') // hash: 'foo', sort: 'bar'
```

## `insert`

Writes an item to the database. The key attributes must be specified.

```js
await ddb.insert({ id: 'foo', name: 'john' })

// The inserted item or (in case of overwrite) old item can be returned
// by chaining `.returning()`. Valid arguments are 'NEW' and 'OLD'.
const oldUser = await ddb.insert({ id: 'foo', name: 'jane' }).returning('OLD')
console.log(oldUser) // > { id: 'foo', name: 'john' }
```

## `update`

Update an existing item. By default, if the key does not yet exist, it writes a new item to the database. You can not include the key parameters in the updated fields.

```js
await ddb.update('asdf', { name: 'foo' })

// Chain `.returning()` to receive the old or updated item.
// Valid arguments are 'NEW', 'OLD', 'UPDATED_NEW', and 'UPDATED_OLD'.
const oldName = await ddb.update('asdf', { name: 'bar' }).returning('UPDATED_OLD')
console.log(oldName) // > { name: 'foo' }

// Chain `.ifExists()` to prevent creating a new item if the key doesn't exist.
await ddb.update('new_key', { name: 'foo' }).ifExists() // throws error

// with composite key
await ddb.update(['hash', 'sort'], { data: '…' })
```

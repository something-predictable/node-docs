type Context = object

type TableNamesOf<Schema> = keyof Schema & string
type PartitionKeyOf<Schema, Table extends TableNamesOf<Schema>> = keyof Schema[Table] & string
type KeyOf<Schema, Table extends TableNamesOf<Schema>> = keyof Schema[Table][PartitionKeyOf<
    Schema,
    Table
>] &
    string
type DocumentOfTable<Schema, Table extends TableNamesOf<Schema>> = Schema[Table][PartitionKeyOf<
    Schema,
    Table
>][KeyOf<Schema, Table>]

type DocumentOfFixedPartition<
    Schema,
    Table extends TableNamesOf<Schema>,
    P extends PartitionKeyOf<Schema, Table>,
> = Schema[Table][P][KeyOf<Schema, Table>]

type DocumentOfFixedKey<
    Schema,
    Table extends TableNamesOf<Schema>,
    K extends KeyOf<Schema, Table>,
> = Schema[Table][PartitionKeyOf<Schema, Table>][K]

type Tables<Schema> = {
    readonly [P in TableNamesOf<Schema>]: Documents<Schema, P>
}

type Documents<Schema, Table extends TableNamesOf<Schema>> =
    string extends PartitionKeyOf<Schema, Table>
        ? PartitionsWithFixedKey<Schema, Table>
        : {
              readonly [P in PartitionKeyOf<Schema, Table>]: FixedPartition<
                  DocumentOfFixedPartition<Schema, Table, P>
              >
          } & {
              update(row: Row<DocumentOfTable<Schema, Table>>): Promise<void>
          }

type PartitionsWithFixedKey<Schema, Table extends TableNamesOf<Schema>> = {
    withKey<K extends KeyOf<Schema, Table>>(key: K): FixedKey<DocumentOfFixedKey<Schema, Table, K>>
}

type FixedKey<Value> = {
    get: (partition: string) => Promise<Row<Value> | undefined>
    add: (partition: string, document: Value) => Promise<Row<Value>>
}

type FixedPartition<Value> = {
    get: (key: string) => Promise<Row<Value> | undefined>
    add: (key: string, document: Value) => Promise<Row<Value>>
}

type Row<T> = {
    readonly created: Date
    readonly modified: Date
    readonly revision: unknown
    readonly sequenceNumber: number
    readonly document: T
}

type GenericSchema<Document> = {
    [table: string]: {
        [partition: string]: {
            [key: string]: Document
        }
    }
}

export function tables<Schema = GenericSchema<unknown>>(context: Context) {
    const d = _driver
    if (!d) {
        throw new Error('Please call setDriver() before accessing documents.')
    }
    const connection = d.connect(context)
    return new Proxy(tablesBase(connection), tablesProxy) as unknown as Tables<Schema> &
        AsyncDisposable
}

const tableNameEntry = Symbol()
const connectionEntry = Symbol()
const documentsBaseEntry = Symbol()
const partitionKeyEntry = Symbol()

function tablesBase(connection: Promise<Connection>) {
    return {
        [connectionEntry]: connection,
        [Symbol.asyncDispose]: async () => {
            const c = await connection
            await c.close()
        },
    }
}

const tablesProxy = {
    get: (
        target: { [k: string | symbol]: unknown } & ReturnType<typeof tablesBase>,
        property: string | symbol,
    ) => {
        if (property in target) {
            return target[property]
        }
        if (typeof property === 'symbol') {
            return undefined
        }
        return new Proxy<{ [k: string | symbol]: unknown }>(tableBase(target, property), tableProxy)
    },
}

function tableBase(db: ReturnType<typeof tablesBase>, table: string) {
    return {
        [connectionEntry]: db[connectionEntry],
        [tableNameEntry]: table,
        [Symbol.asyncDispose]: async () => {
            const c = await db[connectionEntry]
            await c.close()
        },
        withKey: (key: string) => {
            return Promise.resolve(`document in ${table} with key ${key}`)
        },
    }
}

const tableProxy = {
    get: (
        target: { [k: string | symbol]: unknown } & ReturnType<typeof tableBase>,
        property: string | symbol,
    ) => {
        if (property in target) {
            return target[property]
        }
        if (typeof property === 'symbol') {
            return undefined
        }
        return new Proxy<{ [k: string | symbol]: unknown }>(
            partitionBase(target, property),
            partitionProxy,
        )
    },
}

function partitionBase(db: ReturnType<typeof tableBase>, partition: string) {
    return {
        [documentsBaseEntry]: db,
        [partitionKeyEntry]: partition,
        get: (key: string) => {
            return Promise.resolve(`document at ${db[tableNameEntry]}.${partition}.${key}`)
        },
        add: (_key: string, _value: unknown) => {
            return Promise.resolve()
        },
    }
}

const partitionProxy = {
    get: (
        target: { [k: string | symbol]: unknown } & ReturnType<typeof partitionBase>,
        property: string | symbol,
    ) => {
        if (property in target) {
            return target[property]
        }
        if (typeof property === 'symbol') {
            return undefined
        }
        return {
            get: () =>
                `document at ${target[documentsBaseEntry][tableNameEntry]}.${target[partitionKeyEntry]}.${property}`,
        }
    },
}

type Driver = {
    connect: (context: Context) => Promise<Connection>
}
type Connection = {
    close: () => Promise<void>
}

let _driver: Driver | undefined

export function setDriver(driver: Driver) {
    _driver = driver
}

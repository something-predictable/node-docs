type Context = object

type PartitionKeyOf<Schema> = keyof Schema
type KeyOf<Schema> = keyof Schema[PartitionKeyOf<Schema>]
export type DocumentOf<Schema> = Schema[PartitionKeyOf<Schema>][KeyOf<Schema>]

type Documents<Schema> = {
    readonly [P in keyof Schema]-?: PartitionKeyOf<Schema> extends string
        ? Partition<Schema[P][keyof Schema[P]]>
        : FixedPartition<Schema[P]>
} & {
    update(row: Row<DocumentOf<Schema>>): Promise<void>
}

type Partition<Value> = {
    get: (key: string) => Promise<Row<Value> | undefined>
    add: (key: string, document: Value) => Promise<Row<Value>>
}

type FixedPartition<P> = {
    readonly [K in keyof P]-?: Document<P[K]>
}

type Document<Value> = {
    get(): Promise<Row<Value> | undefined>
    put(d: Value): Promise<void>
    update(d: Row<Value>): Promise<void>
}

type Row<T> = {
    readonly created: Date
    readonly modified: Date
    readonly revision: unknown
    readonly sequenceNumber: number
    readonly document: T
}

export function documents<Schema>(context: Context, table: string) {
    const d = _driver
    if (!d) {
        throw new Error('Please call setDriver() before accessing documents.')
    }
    const connection = d.connect(context)
    return new Proxy(
        documentsBase(connection, table),
        documentsProxy,
    ) as unknown as Documents<Schema> & AsyncDisposable
}

const tableNameEntry = Symbol()
const connectionEntry = Symbol()
const documentsBaseEntry = Symbol()
const partitionKeyEntry = Symbol()

function documentsBase(connection: Promise<Connection>, table: string) {
    return {
        [connectionEntry]: connection,
        [tableNameEntry]: table,
        [Symbol.asyncDispose]: async () => {
            const c = await connection
            await c.close()
        },
    }
}

const documentsProxy = {
    get: (
        target: { [k: string | symbol]: unknown } & ReturnType<typeof documentsBase>,
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

function partitionBase(db: ReturnType<typeof documentsBase>, partition: string) {
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

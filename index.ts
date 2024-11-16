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

export type Revision = unknown

export type Row<T> = {
    readonly revision: Revision
    readonly document: T
}

type StoreDocument = unknown

type FixedKey<Document> = {
    add: (partition: string, document: Document) => Promise<Revision>
    get: (partition: string) => Promise<Row<Document> | undefined>
    update: (partition: string, revision: Revision, document: Document) => Promise<Revision>
    delete: (partition: string, revision: Revision) => Promise<void>
}

type FixedPartition<Document> = {
    add: (key: string, document: Document) => Promise<Revision>
    get: (key: string) => Promise<Row<Document> | undefined>
    update: (key: string, revision: Revision, document: Document) => Promise<Revision>
    delete: (key: string, revision: Revision) => Promise<void>
}

type GenericSchema = {
    [table: string]: {
        [partition: string]: {
            [key: string]: StoreDocument
        }
    }
}

export function tables<Schema = GenericSchema>(context: Context) {
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

type GenericProxyTarget = { [k: string | symbol]: unknown }

const tablesProxy = {
    get: (
        target: GenericProxyTarget & ReturnType<typeof tablesBase>,
        property: string | symbol,
    ) => {
        if (property in target) {
            return target[property]
        }
        if (typeof property === 'symbol') {
            return undefined
        }
        return new Proxy(tableBase(target, property), tableProxy)
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
        withKey: (key: string) => ({
            add: async (partition: string, document: StoreDocument) =>
                (await db[connectionEntry]).add(table, partition, key, document),
            get: async (partition: string) =>
                (await db[connectionEntry]).get(table, partition, key),
            update: async (partition: string, revision: string, document: StoreDocument) =>
                (await db[connectionEntry]).update(table, partition, key, revision, document),
            delete: async (partition: string, revision: Revision) =>
                (await db[connectionEntry]).delete(table, partition, key, revision),
        }),
    }
}

const tableProxy = {
    get: (target: GenericProxyTarget & ReturnType<typeof tableBase>, property: string | symbol) => {
        if (property in target) {
            return target[property]
        }
        if (typeof property === 'symbol') {
            return undefined
        }
        return new Proxy(partitionBase(target, property), partitionProxy)
    },
}

function partitionBase(db: ReturnType<typeof tableBase>, partition: string) {
    return {
        [connectionEntry]: db[connectionEntry],
        [tableNameEntry]: db[tableNameEntry],
        [partitionKeyEntry]: partition,
        add: async (key: string, document: StoreDocument) =>
            (await db[connectionEntry]).add(db[tableNameEntry], partition, key, document),
        get: async (key: string) =>
            (await db[connectionEntry]).get(db[tableNameEntry], partition, key),
        update: async (key: string, revision: string, document: StoreDocument) =>
            (await db[connectionEntry]).update(
                db[tableNameEntry],
                partition,
                key,
                revision,
                document,
            ),
        delete: async (key: string, revision: Revision) =>
            (await db[connectionEntry]).delete(db[tableNameEntry], partition, key, revision),
    }
}

const partitionProxy = {
    get: (
        target: GenericProxyTarget & ReturnType<typeof partitionBase>,
        property: string | symbol,
    ) => {
        if (property in target) {
            return target[property]
        }
        if (typeof property === 'symbol') {
            return undefined
        }
        return {
            get: () => {
                throw new Error(
                    `document at ${target[tableNameEntry]}.${target[partitionKeyEntry]}.${property}`,
                )
            },
        }
    },
}

type Driver = {
    connect: (context: Context) => Promise<Connection>
}

type Connection = {
    close: () => Promise<void>
    add: (
        table: string,
        partition: string,
        key: string,
        document: StoreDocument,
    ) => Promise<Revision>
    get: (table: string, partition: string, key: string) => Promise<Row<StoreDocument>>
    update: (
        table: string,
        partition: string,
        key: string,
        revision: Revision,
        document: StoreDocument,
    ) => Promise<Revision>
    delete: (table: string, partition: string, key: string, revision: Revision) => Promise<void>
}

let _driver: Driver | undefined

export function setDriver(driver: Driver) {
    _driver = driver
}

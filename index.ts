type Context = object

type TableNamesOf<Schema> = keyof Schema & string
type PartitionKeyOf<Schema, Table extends TableNamesOf<Schema>> = keyof Schema[Table] & string
type KeyOf<Schema, Table extends TableNamesOf<Schema>> = keyof Schema[Table][PartitionKeyOf<
    Schema,
    Table
>] &
    string
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
        : Partitions<Schema, Table>

type PartitionsWithFixedKey<Schema, Table extends TableNamesOf<Schema>> = {
    withKey<K extends KeyOf<Schema, Table>>(key: K): FixedKey<DocumentOfFixedKey<Schema, Table, K>>
}

type Partitions<Schema, Table extends TableNamesOf<Schema>> = {
    readonly [P in PartitionKeyOf<Schema, Table>]: FixedPartition<
        DocumentOfFixedPartition<Schema, Table, P>
    >
}

export type Revision = unknown

export type Row<T> = {
    readonly revision: Revision
    readonly document: T
}

type StoredDocument = unknown

type FixedKey<Document> = {
    add: (partition: string, document: Document) => Promise<Revision>
    get: (
        partition: string,
    ) => Promise<{ partition: string; revision: Revision; document: Document } | undefined>
    getDocument: (partition: string) => Promise<Document | undefined>
    update: (partition: string, revision: Revision, document: Document) => Promise<Revision>
    updateRow: (row: {
        partition: string
        revision: Revision
        document: Document
    }) => Promise<Revision>
    delete: (partition: string, revision: Revision) => Promise<void>
}

type FixedPartition<Document> = {
    add: (key: string, document: Document) => Promise<Revision>
    get: (
        key: string,
    ) => Promise<{ key: string; revision: Revision; document: Document } | undefined>
    getDocument: (key: string) => Promise<Document | undefined>
    update: (key: string, revision: Revision, document: Document) => Promise<Revision>
    updateRow: (row: { key: string; revision: Revision; document: Document }) => Promise<Revision>
    delete: (key: string, revision: Revision) => Promise<void>
}

type GenericSchema = {
    [table: string]: {
        [partition: string]: {
            [key: string]: StoredDocument
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
        withKey: (key: string): FixedKey<StoredDocument> => ({
            add: async (partition: string, document: StoredDocument) =>
                await (await db[connectionEntry]).add(table, partition, key, document),
            get: async (partition: string) =>
                await (await db[connectionEntry]).get(table, partition, key),
            getDocument: async (partition: string) =>
                (await (await db[connectionEntry]).get(table, partition, key)).document,
            update: async (partition: string, revision: Revision, document: StoredDocument) =>
                await (await db[connectionEntry]).update(table, partition, key, revision, document),
            updateRow: async (row: {
                partition: string
                revision: Revision
                document: StoredDocument
            }) =>
                await (
                    await db[connectionEntry]
                ).update(table, row.partition, key, row.revision, row.document),
            delete: async (partition: string, revision: Revision) => {
                await (await db[connectionEntry]).delete(table, partition, key, revision)
            },
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

function partitionBase(
    db: ReturnType<typeof tableBase>,
    partition: string,
): {
    [tableNameEntry]: string
    [partitionKeyEntry]: string
} & FixedPartition<StoredDocument> {
    return {
        [tableNameEntry]: db[tableNameEntry],
        [partitionKeyEntry]: partition,
        add: async (key: string, document: StoredDocument) =>
            (await db[connectionEntry]).add(db[tableNameEntry], partition, key, document),
        get: async (key: string) =>
            (await db[connectionEntry]).get(db[tableNameEntry], partition, key),
        getDocument: async (key: string) =>
            (await (await db[connectionEntry]).get(db[tableNameEntry], partition, key)).document,
        update: async (key: string, revision: Revision, document: StoredDocument) =>
            (await db[connectionEntry]).update(
                db[tableNameEntry],
                partition,
                key,
                revision,
                document,
            ),
        updateRow: async (row: { key: string; revision: Revision; document: StoredDocument }) =>
            (await db[connectionEntry]).update(
                db[tableNameEntry],
                partition,
                row.key,
                row.revision,
                row.document,
            ),
        delete: async (key: string, revision: Revision) => {
            await (await db[connectionEntry]).delete(db[tableNameEntry], partition, key, revision)
        },
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
        document: StoredDocument,
    ) => Promise<Revision>
    get: (
        table: string,
        partition: string,
        key: string,
    ) => Promise<Row<StoredDocument> & { partition: string; key: string }>
    update: (
        table: string,
        partition: string,
        key: string,
        revision: Revision,
        document: StoredDocument,
    ) => Promise<Revision>
    delete: (table: string, partition: string, key: string, revision: Revision) => Promise<void>
}

let _driver: Driver | undefined

export function setDriver(driver: Driver) {
    _driver = driver
}

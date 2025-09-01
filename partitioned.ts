import { getDriver, type Connection } from './lib/driver.js'
import type { KeyRange, Revision, StoredDocument } from './schema.js'

type Context = {
    on?: (event: 'free', handler: () => Promise<void>) => boolean
}

type TableNamesOf<Schema> = keyof Schema & string
type PartitionKeyOf<Schema, Table extends TableNamesOf<Schema>> = keyof Schema[Table] & string
type KeyOf<
    Schema,
    Table extends TableNamesOf<Schema> = TableNamesOf<Schema>,
> = keyof Schema[Table][PartitionKeyOf<Schema, Table>] & string
type DocumentOfFixedPartition<
    Schema,
    Table extends TableNamesOf<Schema>,
    P extends PartitionKeyOf<Schema, Table>,
> = Schema[Table][P][KeyOf<Schema, Table>]

type DocumentOf<
    Schema,
    Table extends TableNamesOf<Schema> = TableNamesOf<Schema>,
> = Schema[Table][PartitionKeyOf<Schema, Table>][KeyOf<Schema, Table>]

type DocumentOfFixedKey<
    Schema,
    Table extends TableNamesOf<Schema>,
    K extends KeyOf<Schema, Table>,
> = Schema[Table][PartitionKeyOf<Schema, Table>][K]

type Tables<Schema> = string extends TableNamesOf<Schema> ? never : NamedTables<Schema>

type NamedTables<Schema> = {
    readonly [P in TableNamesOf<Schema>]: Documents<Schema, P>
}

type Documents<Schema, Table extends TableNamesOf<Schema>> =
    string extends PartitionKeyOf<Schema, Table>
        ? string extends KeyOf<Schema, Table>
            ? Partitions<Schema, Table>
            : PartitionsWithFixedKey<Schema, Table>
        : NamedPartitions<Schema, Table>

type PartitionsWithFixedKey<Schema, Table extends TableNamesOf<Schema>> = {
    withKey<K extends KeyOf<Schema, Table>>(key: K): FixedKey<DocumentOfFixedKey<Schema, Table, K>>
}

type NamedPartitions<Schema, Table extends TableNamesOf<Schema>> = {
    readonly [P in PartitionKeyOf<Schema, Table>]: NamedPartition<
        DocumentOfFixedPartition<Schema, Table, P>
    >
}

type Partitions<Schema, Table extends TableNamesOf<Schema>> = {
    partition(partition: string): NamedPartition<DocumentOf<Schema, Table>>
}

type FixedKey<Document> = {
    add: (partition: string, document: Document) => Promise<Revision>
    get: (
        partition: string,
    ) => Promise<{ partition: string; revision: Revision; document: Document }>
    getDocument: (partition: string) => Promise<Document>
    update: (partition: string, revision: Revision, document: Document) => Promise<Revision>
    updateRow: (row: {
        partition: string
        revision: Revision
        document: Document
    }) => Promise<Revision>
    delete: (partition: string, revision: Revision) => Promise<void>
}

type NamedPartition<Document> = {
    add: (key: string, document: Document) => Promise<Revision>
    get: (key: string) => Promise<{ key: string; revision: Revision; document: Document }>
    getDocument: (key: string) => Promise<Document>
    getRange: (
        range: KeyRange,
    ) => AsyncIterable<{ key: string; revision: Revision; document: Document }>
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

export function tables<Schema = GenericSchema>(
    context: Context & { on?: undefined },
): Tables<Schema> & AsyncDisposable
export function tables<Schema = GenericSchema>(
    context: Context & { on: (event: 'free', handler: () => Promise<void>) => void },
): Tables<Schema>

export function tables<Schema = GenericSchema>(context: Context) {
    const d = getDriver()
    const connection = d.connect(context)
    const closer = async () => {
        const c = await connection
        await c.close()
    }
    const p = new Proxy(tablesBase(connection), tablesProxy) as unknown as Tables<Schema>
    if (!context.on?.('free', closer)) {
        const dp = p as Tables<Schema> & AsyncDisposable
        dp[Symbol.asyncDispose] = closer
    }
    return p
}

const connectionEntry = Symbol()
const tableNameEntry = Symbol()

function tablesBase(connection: Promise<Connection>) {
    return {
        [connectionEntry]: connection,
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
        withKey: (key: string) => ({
            async add(partition: string, document: unknown) {
                const c = await db[connectionEntry]
                return c.add(table, partition, key, document)
            },
            async get(partition: string) {
                const c = await db[connectionEntry]
                return await c.get(table, partition, key)
            },
            async getDocument(partition: string) {
                const c = await db[connectionEntry]
                const r = await c.get(table, partition, key)
                return r.document
            },
            async update(partition: string, revision: Revision, document: StoredDocument) {
                const c = await db[connectionEntry]
                return await c.update(table, partition, key, revision, document)
            },
            async updateRow(row: {
                partition: string
                revision: Revision
                document: StoredDocument
            }) {
                const c = await db[connectionEntry]
                return await c.update(table, row.partition, key, row.revision, row.document)
            },
            async delete(partition: string, revision: Revision) {
                const c = await db[connectionEntry]
                await c.delete(table, partition, key, revision)
            },
        }),
        partition: (partition: string) => new Partition(db[connectionEntry], table, partition),
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
        return new Partition(target[connectionEntry], target[tableNameEntry], property)
    },
}

class Partition {
    readonly #connection
    readonly #table
    readonly #partition

    constructor(connection: Promise<Connection>, table: string, partition: string) {
        this.#connection = connection
        this.#table = table
        this.#partition = partition
    }

    async add(key: string, document: StoredDocument) {
        const c = await this.#connection
        return c.add(this.#table, this.#partition, key, document)
    }
    async get(key: string) {
        const c = await this.#connection
        return c.get(this.#table, this.#partition, key)
    }
    async getDocument(key: string) {
        const r = await this.get(key)
        return r.document
    }
    async *getRange(range: KeyRange) {
        const c = await this.#connection
        for await (const r of c.getRange(this.#table, this.#partition, range)) {
            yield r
        }
    }
    async update(key: string, revision: Revision, document: StoredDocument) {
        const c = await this.#connection
        return c.update(this.#table, this.#partition, key, revision, document)
    }
    async updateRow(row: { key: string; revision: Revision; document: StoredDocument }) {
        const c = await this.#connection
        return c.update(this.#table, this.#partition, row.key, row.revision, row.document)
    }
    async delete(key: string, revision: Revision) {
        const c = await this.#connection
        await c.delete(this.#table, this.#partition, key, revision)
    }
}

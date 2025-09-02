import { randomUUID } from 'node:crypto'
import type { KeyRange } from './schema.js'

const documentsEntry = Symbol()

export class MemoryDriver {
    connect(context: object) {
        const ext = context as { [documentsEntry]?: MemoryDocuments }
        return Promise.resolve((ext[documentsEntry] ??= new MemoryDocuments()))
    }
}

export class PersistentMemoryDriver {
    readonly #documents = new MemoryDocuments()

    connect() {
        return Promise.resolve(this.#documents)
    }
}

type Row = {
    json: string
    revision: string
}

class MemoryDocuments {
    readonly #tables = new MapWithDefault(() => new MapWithDefault(() => new Map<string, Row>()))
    #closed = false

    async add(table: string, partition: string, key: string, document: unknown) {
        await this.#throwIfClosed()
        const revision = randomUUID()
        const p = this.#tables.get(table).get(partition)
        if (p.get(key)) {
            throw conflict()
        }
        p.set(key, { revision, json: JSON.stringify(document) })
        return revision
    }

    async get(table: string, partition: string, key: string) {
        await this.#throwIfClosed()
        const row = this.#tables.get(table).get(partition).get(key)
        if (!row) {
            throw notFound()
        }
        return {
            partition,
            key,
            revision: row.revision,
            document: JSON.parse(row.json) as unknown,
        }
    }

    async *getPartition(table: string, partition: string, range?: KeyRange) {
        await this.#throwIfClosed()
        const matches = matchRange(range)
        for (const [key, row] of this.#tables.get(table).get(partition)) {
            await Promise.resolve()
            if (matches(key)) {
                yield {
                    key,
                    revision: row.revision,
                    document: JSON.parse(row.json) as unknown,
                }
            }
        }
    }

    async update(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        document: unknown,
    ) {
        await this.#throwIfClosed()
        const p = this.#tables.get(table).get(partition)
        const r = p.get(key)
        if (!r) {
            throw conflict()
        }
        if (r.revision !== currentRevision) {
            throw conflict()
        }
        const revision = randomUUID()
        p.set(key, { revision, json: JSON.stringify(document) })
        return revision
    }

    async delete(table: string, partition: string, key: string, currentRevision: unknown) {
        await this.#throwIfClosed()
        const p = this.#tables.get(table).get(partition)
        const r = p.get(key)
        if (!r) {
            throw conflict()
        }
        if (r.revision !== currentRevision) {
            throw conflict()
        }
        p.delete(key)
    }

    #throwIfClosed() {
        if (this.#closed) {
            return Promise.reject(new Error('Connection has been closed.'))
        }
        return Promise.resolve()
    }

    close() {
        this.#closed = true
        return Promise.resolve()
    }
}

function matchRange(range?: KeyRange) {
    if (!range) {
        return () => true
    }
    if ('withPrefix' in range) {
        return (key: string) => key.startsWith(range.withPrefix)
    }
    if ('before' in range || 'after' in range) {
        const { after, before } = range
        if (after) {
            if (before) {
                return (key: string) => after <= key && key < before
            } else {
                return (key: string) => after <= key
            }
        }
        if (before) {
            return (key: string) => key < before
        }
    }
    return () => false
}

class MapWithDefault<K, V> {
    readonly #map: Map<K, V>
    readonly #default: () => V

    constructor(d: () => V) {
        this.#map = new Map()
        this.#default = d
    }

    get(key: K) {
        const existing = this.#map.get(key)
        if (existing) {
            return existing
        }
        const d = this.#default()
        this.#map.set(key, d)
        return d
    }
}

function conflict() {
    const e = new Error('Conflict')
    ;(e as unknown as { status: number }).status = 409
    return e
}

function notFound() {
    const e = new Error('Not found')
    ;(e as unknown as { status: number }).status = 404
    return e
}

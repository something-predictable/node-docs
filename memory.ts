import { randomUUID } from 'node:crypto'

export class MemoryDriver {
    connect() {
        return Promise.resolve(new MemoryDocuments())
    }
}

type Row = {
    json: string
    revision: string
}

class MemoryDocuments {
    readonly #tables = new MapWithDefault(() => new MapWithDefault(() => new Map<string, Row>()))

    close() {
        return Promise.resolve()
    }

    add(table: string, partition: string, key: string, document: unknown) {
        const revision = randomUUID()
        const p = this.#tables.get(table).get(partition)
        if (p.get(key)) {
            throw conflict()
        }
        p.set(key, { revision, json: JSON.stringify(document) })
        return Promise.resolve(revision)
    }

    get(table: string, partition: string, key: string) {
        const r = this.#tables.get(table).get(partition).get(key)
        if (!r) {
            throw notFound()
        }
        return Promise.resolve({
            partition,
            key,
            revision: r.revision,
            document: JSON.parse(r.json) as unknown,
        })
    }

    update(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        document: unknown,
    ) {
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
        return Promise.resolve(revision)
    }

    delete(table: string, partition: string, key: string, currentRevision: unknown) {
        const p = this.#tables.get(table).get(partition)
        const r = p.get(key)
        if (!r) {
            throw conflict()
        }
        if (r.revision !== currentRevision) {
            throw conflict()
        }
        p.delete(key)
        return Promise.resolve()
    }
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

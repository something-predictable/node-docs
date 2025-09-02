import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import type { Driver } from './lib/driver.js'
import { isConflict, isNotFound } from './partitioned.js'

const table = 'HarnessTestDocs'

export function harness(
    it: (message: string, runner: () => Promise<void>) => void,
    driver: Driver,
    contextFactory: () => object,
) {
    it('throws not found when not added', async () => {
        await using c = await connect(driver, contextFactory)
        await assert.rejects(c.docs.get(table, anId(), anId()), isNotFound)
    })

    it('gets added', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const addedRevision = await c.docs.add(table, partition, key, added)
        const { document, revision } = await c.docs.get(table, partition, key)
        assert.deepStrictEqual(added, document)
        assert.strictEqual(addedRevision, revision)
    })

    it('gets JSON serialized', async () => {
        const now = new Date()
        const { partition, key, document: added } = aRow({ time: now })
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added)
        const { document } = await c.docs.get(table, partition, key)
        assert.strictEqual(added.time.toISOString(), (document as { time: unknown }).time)
    })

    it('rejects second add', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        await c.docs.add(table, partition, key, added)
        await assert.rejects(c.docs.add(table, partition, key, added), isConflict)
    })

    it('gets updated', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const addedRevision = await c.docs.add(table, partition, key, added)
        const updated = aDocument()
        const updatedRevision = await c.docs.update(table, partition, key, addedRevision, updated)
        const { document, revision } = await c.docs.get(table, partition, key)
        assert.deepStrictEqual(updated, document)
        assert.strictEqual(updatedRevision, revision)
    })

    it('rejects updating updated', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const addedRevision = await c.docs.add(table, partition, key, added)
        await c.docs.update(table, partition, key, addedRevision, aDocument())
        await assert.rejects(
            c.docs.update(table, partition, key, addedRevision, aDocument()),
            isConflict,
        )
    })

    it('gets empty range', async () => {
        await using c = await connect(driver, contextFactory)
        const partition = await collectDocuments(c.docs.getPartition(table, anId()))
        assert.deepStrictEqual(partition, [])
    })

    it('gets ranges', async () => {
        await using c = await connect(driver, contextFactory)
        const partition = anId()
        await c.docs.add(table, partition, 'a1', aDocument({ key: 'a1' }))
        await c.docs.add(table, partition, 'a2', aDocument({ key: 'a2' }))
        await c.docs.add(table, partition, 'b', aDocument({ key: 'b' }))
        await c.docs.add(table, partition, 'c1', aDocument({ key: 'c1' }))
        await c.docs.add(table, partition, 'c2', aDocument({ key: 'c2' }))
        await c.docs.add(table, partition, 'c3', aDocument({ key: 'c3' }))
        assert.deepStrictEqual(
            (await collectDocuments(c.docs.getPartition(table, partition, { before: 'b' }))).map(
                d => (d as { key: string }).key,
            ),
            ['a1', 'a2'],
        )
        assert.deepStrictEqual(
            (await collectDocuments(c.docs.getPartition(table, partition, { after: 'b' }))).map(
                d => (d as { key: string }).key,
            ),
            ['b', 'c1', 'c2', 'c3'],
        )
        assert.deepStrictEqual(
            (await collectDocuments(c.docs.getPartition(table, partition, { after: 'c' }))).map(
                d => (d as { key: string }).key,
            ),
            ['c1', 'c2', 'c3'],
        )
        assert.deepStrictEqual(
            (
                await collectDocuments(
                    c.docs.getPartition(table, partition, { after: 'a', before: 'c' }),
                )
            ).map(d => (d as { key: string }).key),
            ['a1', 'a2', 'b'],
        )
        assert.deepStrictEqual(
            (
                await collectDocuments(c.docs.getPartition(table, partition, { withPrefix: 'a' }))
            ).map(d => (d as { key: string }).key),
            ['a1', 'a2'],
        )
    })

    it('deletes added', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const addedRevision = await c.docs.add(table, partition, key, added)
        await c.docs.delete(table, partition, key, addedRevision)
        await assert.rejects(c.docs.get(table, partition, key), isNotFound)
    })

    it('re-adds deleted', async () => {
        const { partition, key, document: added } = aRow()
        await using c = await connect(driver, contextFactory)
        const addedRevision = await c.docs.add(table, partition, key, added)
        await c.docs.delete(table, partition, key, addedRevision)
        const reAddedRevision = await c.docs.add(table, partition, key, added)
        const { document, revision } = await c.docs.get(table, partition, key)
        assert.deepStrictEqual(added, document)
        assert.strictEqual(reAddedRevision, revision)
    })
}

export async function collectDocuments<T>(range: AsyncIterable<{ document: T }>) {
    const collected = []
    for await (const { document } of range) {
        collected.push(document)
    }
    return collected
}

function anId(): string {
    return randomUUID()
}

function aDocument<T extends { [key: string]: unknown }>(props?: T) {
    return {
        data: anId(),
        ...props,
    } as T & { data: string }
}

function aRow<T extends { [key: string]: unknown }>(props?: T) {
    return { table, partition: anId(), key: anId(), document: aDocument(props) } as const
}

async function connect(driver: Driver, contextFactory: () => object) {
    const connection = await driver.connect(contextFactory())
    return {
        docs: connection,
        [Symbol.asyncDispose]: () => connection.close(),
    }
}

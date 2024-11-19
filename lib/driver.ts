import type { KeyRange, Revision, Row, StoredDocument } from '../schema.js'

export type Context = object

export type Driver = {
    connect: (context: Context) => Promise<Connection>
}

export type Connection = {
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
    ) => Promise<(Row<StoredDocument> & { partition: string; key: string }) | undefined>
    getRange: (
        table: string,
        partition: string,
        keyRange: KeyRange,
    ) => AsyncIterable<{ key: string; revision: Revision; document: StoredDocument }>
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

export function getDriver() {
    return _driver
}

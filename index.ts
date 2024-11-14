import { setTimeout } from 'node:timers/promises'

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

export function documents<Schema>(_context: Context, table: string) {
    return new Proxy<{ [k: string | symbol]: unknown }>(
        {
            [Symbol.asyncDispose]: async () => {
                await setTimeout(1)
            },
        },
        {
            get: (target, property) => {
                if (property in target) {
                    return target[property]
                }
                return new Proxy<{ [k: string | symbol]: unknown }>(
                    {
                        get: () => {
                            return 0
                        },
                    },
                    {
                        get: (__, key: string) => {
                            if (property in target) {
                                return target[property]
                            }
                            return `document at ${table}.${property as string}.${key}`
                        },
                    },
                )
            },
        },
    ) as Documents<Schema> & AsyncDisposable
}

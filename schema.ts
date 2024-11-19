export type KeyRange =
    | {
          withPrefix: string
      }
    | {
          before?: string
          after: string
      }
    | {
          before: string
          after?: string
      }

export type StoredDocument = unknown

export type Revision = unknown

export type Row<T> = {
    readonly revision: Revision
    readonly document: T
}

// type MySchema = {
//     a: { count: number }
//     b: { name: string; size: number }
// }

// type Defs1<Schema> = {
//     [name: string]: Def1<Schema>
// }

// type Def1<Schema> = {
//     source: (x: Schema) => unknown
// }

// type Defs2<Schema> = {
//     [name: string]: Def2<Schema>
// }

// type Def2<Schema> = Temp<Schema>[keyof Schema]

// type MyDef2 = Def2<MySchema>

// type Temp<Schema> = {
//     [P in keyof Schema]: DefMatch<Schema, Schema[P]>
// }

// type DefMatch<Schema, Document> = {
//     source: (x: Schema) => Document
//     projection: (d: Document) => number
// }

// function tables<Schema>(_context: object, _d: Defs2<Schema>) {
//     //
// }

// tables<MySchema>(
//     {},
//     {
//         ix: {
//             source: t => t.a,
//             projection: d => d.count,
//         },
//     },
// )

// function f<Document>(_: DefMatch<MySchema, Document>) {
//     //
// }

// f({
//     source: t => t.a,
//     projection: d => d.count,
// })

// type O = Temp<MySchema>[keyof MySchema]
// function g(_: DefMatch<MySchema, MySchema['a']> | DefMatch<MySchema, MySchema['b']>) {
//     //
// }

// g({
//     source: t => t.a,
//     projection: d => d.count,
// })

// // type IndexDefinitions<Schema> = {
// //     [index: string]: IndexDefinition<Schema>
// // }

// type UsersSchema = {
//     UserDocs: {
//         [id: string]: {
//             profile: {
//                 name: string
//                 email: string
//             }
//             invitations: {
//                 id: string
//                 scopes: string[]
//             }[]
//         }
//     }
// }

// type IndexSourceSelector<Schema> = {
//     readonly [P in TableNamesOf<Schema>]: IndexSourceDocumentsSelector<Schema, P>
// }
// type IndexSourceDocumentsSelector<Schema, Table extends TableNamesOf<Schema>> =
//     string extends PartitionKeyOf<Schema, Table>
//         ? string extends KeyOf<Schema, Table>
//             ? never // Partitions<Schema, Table>
//             : IndexSourceDocumentsSelectorWithFixedKey<Schema, Table>
//         : never // NamedPartitions<Schema, Table>

// type IndexSourceDocumentsSelectorWithFixedKey<Schema, Table extends TableNamesOf<Schema>> = {
//     withKey<K extends KeyOf<Schema, Table>>(key: K): FixedKeySelection<Schema, Table, K>
// }

// type FixedKeySelection<Schema, Table, Key> = {
//     schema: Schema
//     table: Table
//     key: Key
// }

// type FixedKeyStuff<
//     Schema,
//     Table, // extends TableNamesOf<Schema>,
//     Key, // extends KeyOf<Schema, Table>,
// > = {
//     f: (t: IndexSourceSelector<Schema>) => FixedKeySelection<Schema, Table, Key>
// }

// type Inner<Schema, S> =
//     S extends FixedKeyStuff<
//         Schema,
//         infer Table, // extends TableNamesOf<Schema>,
//         infer Key // extends KeyOf<Schema, infer Table>
//     >
//         ? {
//               table: Table
//               key: Key
//           }
//         : never

// const a = {
//     f: (t: IndexSourceSelector<UsersSchema>) => t.UserDocs.withKey('profile'),
// }

// function fn<Table, Key>(_: FixedKeyStuff<UsersSchema, Table, Key>) {
//     //
// }

// class G<Schema> {
//     gn<Table, Key>(_: FixedKeyStuff<Schema, Table, Key>): FixedKeyStuff<Schema, Table, Key> {
//         return _
//     }
// }

// fn(a)

// fn({
//     f: t => t.UserDocs.withKey('profile'),
// })

// new G<UsersSchema>().gn({
//     f: t => t.UserDocs.withKey('profile'),
// })

// function def<Schema>(_tables: Tables<Schema>, _x?: (b: G<Schema>) => void) {
//     //
// }

// def(tables<UsersSchema>({}), b =>
//     b.gn({
//         f: t => t.UserDocs.withKey('profile'),
//     }),
// )

// const ix = {
//     A: a,
// }

// type IxDef = Inner<UsersSchema, typeof ix.A>

// export const x: IxDef = {
//     table: 'UserDocs',
//     key: 'profile',
//     // document: 2,
// }

// // function fn<S>(t: S): Inner<S> {
// //     const x = t.f()
// //     t.g(x)
// //     return x
// // }

// // type IndexOfFixedKeyDefinition<
// //     Schema,
// //     FixedKeySource extends FixedKey<DocumentOfFixedKey<Schema, TableNamesOf<Schema>, K>>,
// // > = {
// //     source: (t: Tables<Schema, NoIndices>) => FixedKeySource
// // }

// // type IndexDefinition<Schema> = <K>(
// //     // table: FixedKey<K, Document>,
// //     source: (
// //         t: Tables<Schema, NoIndices>,
// //     ) => FixedKey<K, DocumentOfFixedKey<Schema, TableNamesOf<Schema>, K>>,
// //     partitionSelector: (d: DocumentOfFixedKey<Schema, TableNamesOf<Schema>, K>) => string,
// //     keySelector: (d: DocumentOfFixedKey<Schema, TableNamesOf<Schema>, K>) => string,
// // ) => GlobalLookupFixedKeyIndex

// // type IndexDefinition<Schema> = {
// //     // table: FixedKey<K, Document>,
// //     source: (
// //         t: Tables<Schema, NoIndices>,
// //     ) => FixedKey<K, DocumentOfFixedKey<Schema, TableNamesOf<Schema>, K>>
// //     partitionSelector: (d: DocumentOfFixedKey<Schema, TableNamesOf<Schema>, K>) => string
// //     keySelector: (d: DocumentOfFixedKey<Schema, TableNamesOf<Schema>, K>) => string
// // }

// // type GlobalLookupFixedKeyIndex = {
// //     get: (partition: string, key: string) => Promise<{ partition: string; key: string }>
// // }

// // export function lookupIndex<Schema, K extends KeyOf<Schema>>(
// //     // table: FixedKey<K, Document>,
// //     source: (
// //         t: Tables<Schema, NoIndices>,
// //     ) => FixedKey<K, DocumentOfFixedKey<Schema, TableNamesOf<Schema>, K>>,
// //     partitionSelector: (d: DocumentOfFixedKey<Schema, TableNamesOf<Schema>, K>) => string,
// //     keySelector: (d: DocumentOfFixedKey<Schema, TableNamesOf<Schema>, K>) => string,
// // ): GlobalLookupFixedKeyIndex {
// //     throw new Error('Function not implemented.')
// // }

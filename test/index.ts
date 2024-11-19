import assert from 'node:assert/strict'
import { setDriver } from '../driver.js'
import { tables } from '../index.js'
import { index } from '../indexed.js'
import { MemoryDriver } from '../memory.js'

describe('in-memory docs', () => {
    beforeEach(setMemoryDriver)

    it('should get company settings', async () => {
        type CompanyProfilesSchema = {
            CompanyDocs: {
                settings: {
                    [companyId: string]: {
                        website: string
                        count: number
                    }
                }
                keys: {
                    [companyId: string]: {
                        secret: string
                    }
                }
            }
        }
        function getSettings(context: TestContext) {
            return tables<CompanyProfilesSchema>(context).CompanyDocs.settings
        }
        function getKeys(context: TestContext) {
            return tables<CompanyProfilesSchema>(context).CompanyDocs.keys
        }
        function getCompaniesByDomain(context: TestContext) {
            return (
                index<CompanyProfilesSchema>(context, 'settingsByWebsite')
                    .CompanyDocs.settings // Here we can only chose a document property, since partition ('settings') is given,
                    // and the original table is already sorted by key, so no need to do that again
                    .by(d => d.website)
            )
        }

        await using context = new TestContext()
        const companyId = 'some-id'

        const settings = getSettings(context)
        await settings.add(companyId, { website: 'abc.com', count: 3 })
        const row = await settings.get(companyId)
        if (!row) {
            throw new Error('not found')
        }
        const d = row.document
        d.count += 1
        const rev = await settings.updateRow(row)
        d.count += 1
        await settings.update(companyId, rev, d)
        assert.deepStrictEqual(await settings.getDocument(companyId), {
            website: 'abc.com',
            count: 5,
        })

        const keys = getKeys(context)
        await keys.add(companyId, { secret: 'shh!' })
        const keyRow = await keys.get(companyId)
        if (!keyRow) {
            throw new Error('not found')
        }
        keyRow.document.secret = 'yhm'
        await keys.updateRow(keyRow)

        await settings.add('another-id', { website: 'xyz.com', count: 2 })
        await keys.add('another-id', { secret: 'shh!!1!' })

        assert.deepStrictEqual(
            await collectDocuments(settings.getRange({ withPrefix: 'another' })),
            [
                {
                    website: 'xyz.com',
                    count: 2,
                },
            ],
        )

        const companies = await collectDocuments(
            getCompaniesByDomain(context).getRange({ withPrefix: 'www.' }),
        )
        assert.deepStrictEqual(companies, [])
    })

    it('should get user profiles', async () => {
        type UsersSchema = {
            UserDocs: {
                [id: string]: {
                    profile: {
                        name: string
                        email: string
                    }
                    invitations: {
                        id: string
                        scopes: string[]
                    }[]
                }
            }
        }
        function getTables(context: TestContext) {
            return tables<UsersSchema>(context)
        }
        function getProfiles(context: TestContext) {
            return getTables(context).UserDocs.withKey('profile')
        }
        function getInvitations(context: TestContext) {
            return getTables(context).UserDocs.withKey('invitations')
        }
        function getProfilesByEmail(context: TestContext) {
            return index<UsersSchema>(context, 'profilesByEmail')
                .UserDocs.withKey('profile')
                .by(
                    _ => 'profiles',
                    // Here we can only chose a document property, since key ('profile') is given,
                    // and so partition must be the actual unique random ID, which we assume does not
                    // make sense to sort by
                    profile => profile.email,
                ).profiles
        }
        function getNamesByEmail(context: TestContext) {
            return index<UsersSchema>(context, 'namesByEmail')
                .UserDocs.withKey('profile')
                .by(
                    profile => profile.name,
                    profile => profile.email,
                )
        }

        await using context = new TestContext()

        const profiles = getProfiles(context)
        const userId = 'some-id'

        await profiles.add(userId, { name: 'bla', email: 'bla' })

        const invitations = getInvitations(context)
        await invitations.add('invitation ID', [{ id: '', scopes: [] }])

        const emailProfiles = await collectDocuments(
            getProfilesByEmail(context).getRange({ withPrefix: 'doe@' }),
        )
        assert.deepStrictEqual(emailProfiles[0]?.name, 'John')

        const names = await collectDocuments(
            getNamesByEmail(context).partition('John').getRange({ withPrefix: 'doe@' }),
        )
        assert.deepStrictEqual(names[0]?.email, 'doe@abc')
    })

    it('should get messages', async () => {
        type ConversationSchema = {
            Conversations: {
                [userId: string]: {
                    [messageId: string]: {
                        timestamp: string
                        subject: string
                        body: string
                    }
                }
            }
        }
        await using context = new TestContext()

        const conversations = tables<ConversationSchema>(context).Conversations
        const userId = 'some-id'
        const userMessages = conversations.partition(userId)
        await userMessages.add('b', {
            timestamp: '2024-11-19T12:30:00',
            subject: 'huh',
            body: 'hello',
        })
        const range = await collectDocuments(userMessages.getRange({ after: 'a' }))
        assert.deepStrictEqual(range, [
            {
                timestamp: '2024-11-19T12:30:00',
                subject: 'huh',
                body: 'hello',
            },
        ])

        const messagesById = index<ConversationSchema>(context, 'messagesById').Conversations.by(
            _ => 'messages',
            row => row.key,
        ).messages

        const message = await messagesById.getDocument('b')
        assert.strictEqual(message?.subject, 'huh')

        const userMessagesByTimestamp = index<ConversationSchema>(
            context,
            'userMessagesByTimestamp',
        ).Conversations.by(
            row => row.partition,
            row => row.document.timestamp,
        )

        const thisYear = await collectDocuments(
            userMessagesByTimestamp.partition('some-id').getRange({ withPrefix: '2024-' }),
        )
        assert.strictEqual(thisYear.length, 1)

        const subjectLookup = index<ConversationSchema>(
            context,
            'userMessagesByTimestamp',
        ).Conversations.by(
            row => row.document.subject,
            row => row.key,
        )

        const messageIdsWithSubject = await subjectLookup.partition('huh').getDocument('b')
        assert.strictEqual(messageIdsWithSubject?.body, 'hello')
    })
})

async function collectDocuments<T>(range: AsyncIterable<{ document: T }>) {
    const collected = []
    for await (const r of range) {
        collected.push(r.document)
    }
    return collected
}

class TestContext {
    readonly #releasers: (() => Promise<void>)[] = []

    on(event: string, handler: () => Promise<void>) {
        switch (event) {
            case 'free':
                this.#releasers.push(handler)
                return true
        }
        return false
    }

    async [Symbol.asyncDispose]() {
        await Promise.allSettled(this.#releasers.map(r => r()))
    }
}

function setMemoryDriver() {
    setDriver(new MemoryDriver())
}

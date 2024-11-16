import assert from 'node:assert/strict'
import { setDriver, tables } from '../index.js'
import { MemoryDriver } from '../memory.js'

describe('in-memory docs', () => {
    beforeEach(setMemoryDriver)

    it('should get company settings', async () => {
        type CompanyProfilesSchema = {
            CompanyDocs: {
                settings: {
                    [companyId: string]: {
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
        function getSettings(c: TestContext) {
            return tables<CompanyProfilesSchema>(c).CompanyDocs.settings
        }
        function getKeys(c: TestContext) {
            return tables<CompanyProfilesSchema>(c).CompanyDocs.keys
        }

        await using context = new TestContext()
        const companyId = 'some-id'

        const settings = getSettings(context)
        await settings.add(companyId, { count: 3 })
        const row = await settings.get(companyId)
        if (!row) {
            throw new Error('not found')
        }
        const d = row.document
        d.count += 1
        const rev = await settings.updateRow(row)
        d.count += 1
        await settings.update(companyId, rev, d)
        assert.deepStrictEqual(await settings.getDocument(companyId), { count: 5 })

        const keys = getKeys(context)
        await keys.add(companyId, { secret: 'shh!' })
        const keyRow = await keys.get(companyId)
        if (!keyRow) {
            throw new Error('not found')
        }
        keyRow.document.secret = 'yhm'
        await keys.updateRow(keyRow)

        await settings.add('another-id', { count: 2 })
        await keys.add('another-id', { secret: 'shh!!1!' })

        assert.deepStrictEqual(
            await collectDocuments(settings.getRange({ withPrefix: 'another' })),
            [{ count: 2 }],
        )
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
        function getProfiles(context: TestContext) {
            return tables<UsersSchema>(context).UserDocs.withKey('profile')
        }
        function getInvitations(context: TestContext) {
            return tables<UsersSchema>(context).UserDocs.withKey('invitations')
        }

        await using context = new TestContext()

        const profiles = getProfiles(context)
        const userId = 'some-id'

        await profiles.add(userId, { name: 'bla', email: 'bla' })

        const invitations = getInvitations(context)
        await invitations.add('invitation ID', [{ id: '', scopes: [] }])
    })

    it('should get user profiles', async () => {
        type ConversationSchema = {
            Conversations: {
                [userId: string]: {
                    [messageId: string]: {
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
        await userMessages.add('b', { subject: 'huh', body: 'hello' })
        const range = await collectDocuments(userMessages.getRange({ after: 'a' }))
        assert.deepStrictEqual(range, [{ subject: 'huh', body: 'hello' }])
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

import assert from 'node:assert/strict'
import { setDriver, tables } from '../index.js'
import { MemoryDriver } from '../memory.js'

const context = {}

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

        const companies = tables<CompanyProfilesSchema>(context).CompanyDocs
        const companyId = 'some-id'

        await companies.settings.add(companyId, { count: 3 })
        const s = await companies.settings.get(companyId)
        if (!s) {
            throw new Error('not found')
        }
        const d = s.document
        d.count += 1
        const r = await companies.settings.updateRow(s)
        d.count += 1
        await companies.settings.update(companyId, r, d)
        assert.deepStrictEqual(await companies.settings.getDocument(companyId), { count: 5 })

        await companies.keys.add(companyId, { secret: 'shh!' })
        const k = await companies.keys.get(companyId)
        if (!k) {
            throw new Error('not found')
        }
        k.document.secret = 'yhm'
        await companies.keys.updateRow(k)

        await companies.settings.add('another-id', { count: 2 })
        await companies.keys.add('another-id', { secret: 'shh!!1!' })
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

        const profiles = tables<UsersSchema>(context).UserDocs.withKey('profile')
        const userId = 'some-id'

        await profiles.add(userId, { name: 'bla', email: 'bla' })

        const invitations = tables<UsersSchema>(context).UserDocs.withKey('invitations')
        await invitations.add('invitation ID', [{ id: '', scopes: [] }])
    })
})

function setMemoryDriver() {
    setDriver(new MemoryDriver())
}

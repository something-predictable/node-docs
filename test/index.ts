import { setDriver, tables } from '../index.js'

const context = {}

describe('in-memory docs', () => {
    beforeEach('Set driver.', () => {
        setDriver({
            connect: () =>
                Promise.resolve({
                    close: () => Promise.resolve(),
                }),
        })
    })

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
        await companies.update(s)

        const k = await companies.keys.get(companyId)
        if (!k) {
            throw new Error('not found')
        }
        k.document.secret = 'yhm'
        await companies.update(k)

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

        const s = await profiles.add(userId, { name: 'bla', email: 'bla' })
        s.document.name += 1

        const invitations = tables<UsersSchema>(context).UserDocs.withKey('invitations')
        await invitations.add('invitation ID', [{ id: '', scopes: [] }])
    })
})

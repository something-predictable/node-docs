import { setDriver, tables } from '../index.js'

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

        await profiles.add(userId, { name: 'bla', email: 'bla' })

        const invitations = tables<UsersSchema>(context).UserDocs.withKey('invitations')
        await invitations.add('invitation ID', [{ id: '', scopes: [] }])
    })
})

function setMemoryDriver() {
    const driver = new MemoryDriver()
    setDriver({
        connect: () => Promise.resolve(driver),
    })
}

class MemoryDriver {
    close() {
        return Promise.resolve()
    }

    add(_table: string, _partition: string, _key: string, _document: unknown) {
        return Promise.resolve('randomUUID()')
    }

    get(_table: string, _partition: string, _key: string) {
        return Promise.resolve({
            revision: 'randomUUID()',
            document: {},
        })
    }

    update(
        _table: string,
        _partition: string,
        _key: string,
        _revision: unknown,
        _document: unknown,
    ) {
        return Promise.resolve('randomUUID()')
    }

    delete(_table: string, _partition: string, _key: string, _revision: unknown) {
        return Promise.resolve()
    }
}

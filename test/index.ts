import { documents, setDriver } from '../index.js'

export type Generic<Document> = {
    [partition: string]: {
        [key: string]: Document
    }
}

type Companies = {
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

type Users = {
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
        await using companies = documents<Companies>(context, 'CompanyDocs')
        const companyId = 'some-id'

        await companies.settings.add(companyId, { count: 4 })
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
        await using docs = documents<Users>(context, 'UserDocs')
        const userId = 'some-id'

        const s = await docs[userId]?.profile.get()
        if (!s) {
            throw new Error('not found')
        }
        s.document.name += 1
    })
})

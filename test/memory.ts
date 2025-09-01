import { harness } from '../harness.js'
import { MemoryDriver } from '../memory.js'

describe('in-memory driver', () => {
    harness(it, new MemoryDriver(), () => ({}))
})

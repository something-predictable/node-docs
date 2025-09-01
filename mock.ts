import { setDriver, type Driver } from './lib/driver.js'
import { PersistentMemoryDriver } from './memory.js'

let previous: Driver

export const mochaHooks = {
    beforeEach() {
        previous = setDriver(new PersistentMemoryDriver())
    },
    afterEach() {
        setDriver(previous)
    },
}

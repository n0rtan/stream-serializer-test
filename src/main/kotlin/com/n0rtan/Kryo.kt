package com.n0rtan

import org.objenesis.strategy.StdInstantiatorStrategy

class Kryo : com.esotericsoftware.kryo.Kryo() {
    init {
        instantiatorStrategy = StdInstantiatorStrategy()
    }
}
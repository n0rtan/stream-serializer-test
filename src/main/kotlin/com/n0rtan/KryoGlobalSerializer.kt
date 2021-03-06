package com.n0rtan

import java.io.ByteArrayOutputStream
import java.io.IOException
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.serialization.StreamSerializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

class KryoGlobalSerializer : StreamSerializer<Any> {

    private val mapper = ThreadLocal.withInitial { Kryo() }

    /**
     * @return typeId of serializer
     */
    override fun getTypeId(): Int = 1

    /**
     * Called when instance is shutting down. It can be used to clear used resources.
     */
    override fun destroy() {}

    /**
     * This method writes object to ObjectDataOutput
     *
     * @param out    ObjectDataOutput stream that object will be written to
     * @param obj that will be written to out
     * @throws IOException in case of failure to write
     */
    override fun write(out: ObjectDataOutput, obj: Any) {
        Output(ByteArrayOutputStream()).use {
            mapper.get().writeClassAndObject(it, obj)
            out.writeByteArray(it.toBytes())
        }
    }

    /**
     * Reads object from objectDataInputStream
     *
     * @param input ObjectDataInput stream that object will read from
     * @return read object
     * @throws IOException in case of failure to read
     */
    override fun read(input: ObjectDataInput): Any {
        Input(input.readByteArray()).use {
            return mapper.get().readClassAndObject(it)
        }
    }
}
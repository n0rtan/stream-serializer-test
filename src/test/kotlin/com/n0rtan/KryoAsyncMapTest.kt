package com.n0rtan

import java.io.Serializable
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*
import com.hazelcast.config.Config
import com.hazelcast.config.GlobalSerializerConfig
import io.vertx.core.*
import io.vertx.core.shareddata.AsyncMap
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.core.VertxOptions
import io.vertx.core.shareddata.Shareable
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager

@ExtendWith(VertxExtension::class)
class KryoAsyncMapTest {

    companion object {
        private const val MAP_KEY = "__map"
    }

    private lateinit var vertx: Vertx

    private lateinit var map: AsyncMap<String, TestObject>

    private val vertxOptions: VertxOptions = VertxOptions()

    private val testObjectInstance = TestObject("1", 2)

    private val testObjectSerialized = "TestObject(prop1=1, prop2=2)"

    init {
        Config().let { config ->
            with(config.serializationConfig) {
                GlobalSerializerConfig().let {
                    it.implementation = KryoGlobalSerializer()
                    it.isOverrideJavaSerialization = true
                    globalSerializerConfig = it
                }
            }
            vertxOptions.clusterManager = HazelcastClusterManager(config)
        }
    }

    @BeforeEach
    @DisplayName("Run clustered Vertx instance")
    fun prepare(context: VertxTestContext) {
        runBlocking {
            vertx = awaitResult { Vertx.clusteredVertx(vertxOptions, it) }
            map   = awaitResult { vertx.sharedData().getClusterWideMap(MAP_KEY, it) }
        }
        context.completeNow()
    }

    @Test
    @DisplayName("\uD83D\uDE80 Test putting data object to async map")
    fun testPutGetObject(context: VertxTestContext) {
        runBlocking {
            putObject("o", testObjectInstance)
            delay(10)
            assertThat( getObject("o").toString() ).isEqualTo(testObjectSerialized)
        }
        context.completeNow()
    }

    @AfterEach
    @DisplayName("Stop the Vertx instance")
    fun stop(context: VertxTestContext) {
        vertx.close { context.completeNow() }
    }

    private suspend fun putObject(key: String, obj: TestObject) {
        awaitResult<Void> { map.put(key, obj, it) }
    }

    private suspend fun getObject(key: String): TestObject {
        return awaitResult { map.get(key, it) }
    }

    data class TestObject(val prop1: String, val prop2: Int) : KryoObject, Serializable, Shareable
}
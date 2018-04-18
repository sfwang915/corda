package net.corda.node.internal.serialization

import com.github.benmanes.caffeine.cache.Caffeine
import com.google.common.collect.LinkedHashMultimap
import net.corda.core.context.Trace
import net.corda.core.serialization.SerializationContext
import net.corda.node.internal.serialization.testutils.*
import net.corda.node.serialization.amqp.RpcServerObservableSerializer
import net.corda.node.services.messaging.ObservableSubscription
import net.corda.node.services.messaging.ObservableSubscriptionMap
import net.corda.nodeapi.internal.serialization.AllWhitelist
import net.corda.nodeapi.internal.serialization.SerializationContextImpl
import net.corda.nodeapi.internal.serialization.amqp.SerializationOutput
import net.corda.nodeapi.internal.serialization.amqp.SerializerFactory
import net.corda.nodeapi.internal.serialization.amqp.amqpMagic
import org.apache.activemq.artemis.api.core.SimpleString
import org.junit.Test
import rx.Observable
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RpcServerObservableSerializerTests {
    val scheme = AMQPTestSerializationScheme()

    private fun subscriptionMap() : ObservableSubscriptionMap {
        val subMap: ObservableSubscriptionMap = Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES)
                .maximumSize(100)
                .build()

        subMap.put(Trace.InvocationId("test1", Instant.now()), ObservableSubscription(TestSubscription()))

        return subMap
    }

    @Test
    fun canSerializerBeRegistered() {
        val sf = SerializerFactory(cl = javaClass.classLoader, whitelist = AllWhitelist)
        sf.register(RpcServerObservableSerializer(scheme))
    }

    @Test
    fun canAssociateWithContext() {
        val properties : MutableMap<Any, Any> = mutableMapOf()
        val context = SerializationContextImpl(
                preferredSerializationVersion = amqpMagic,
                deserializationClassLoader = javaClass.classLoader,
                whitelist = AllWhitelist,
                properties = properties,
                objectReferencesEnabled = false,
                useCase = SerializationContext.UseCase.Testing,
                encoding = null)

        val observable = TestObservableContext(
                subscriptionMap(),
                clientAddressToObservables = LinkedHashMultimap.create(),
                deduplicationIdentity = "thisIsATest",
                clientAddress = SimpleString ("clientAddress"))

        val newContext = RpcServerObservableSerializer.createContext(observable, context)

        assertEquals(1, newContext.properties.size)
        assertTrue(newContext.properties.containsKey(RpcServerObservableSerializer.RpcObservableContextKey))
        assertEquals(observable, newContext.properties[RpcServerObservableSerializer.RpcObservableContextKey])
    }

    @Test
    fun serialiseFakeObservable() {
        val properties : MutableMap<Any, Any> = mutableMapOf()
        val context = SerializationContextImpl(
                preferredSerializationVersion = amqpMagic,
                deserializationClassLoader = javaClass.classLoader,
                whitelist = AllWhitelist,
                properties = properties,
                objectReferencesEnabled = false,
                useCase = SerializationContext.UseCase.Testing,
                encoding = null)

        val observable = TestObservableContext(
                subscriptionMap(),
                clientAddressToObservables = LinkedHashMultimap.create(),
                deduplicationIdentity = "thisIsATest",
                clientAddress = SimpleString ("clientAddress"))

        val newContext = RpcServerObservableSerializer.createContext(observable, context)

        val sf = SerializerFactory(cl = javaClass.classLoader, whitelist = AllWhitelist).apply {
            register(RpcServerObservableSerializer(scheme, newContext))
        }

        val obs = Observable.create<Int>( { 12 })

        val bytesAndSchemas = SerializationOutput(sf).serializeAndReturnSchema(obs)
        bytesAndSchemas.schema.types.forEach {
            println (it)
        }
    }

}
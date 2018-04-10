package net.corda.node.serialization.amqp

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import net.corda.core.context.Trace
import net.corda.node.serialization.kryo.RpcServerObservableSerializer
import net.corda.node.services.messaging.ObservableSubscription
import net.corda.node.services.messaging.RPCServer
import net.corda.nodeapi.RPCApi
import net.corda.nodeapi.internal.serialization.amqp.*
import org.apache.qpid.proton.codec.Data
import rx.Notification
import rx.Observable
import rx.Subscriber
import java.lang.reflect.Type

/*
object PrivateKeySerializer : CustomSerializer.Implements<PrivateKey>(PrivateKey::class.java) {

    private val allowedUseCases = EnumSet.of(Storage, Checkpoint)

    override val schemaForDocumentation = Schema(listOf(RestrictedType(type.toString(), "", listOf(type.toString()), SerializerFactory.primitiveTypeName(ByteArray::class.java)!!, descriptor, emptyList())))

    override fun writeDescribedObject(obj: PrivateKey, data: Data, type: Type, output: SerializationOutput) {
        checkUseCase(allowedUseCases)
        output.writeObject(obj.encoded, data, clazz)
    }

    override fun readObject(obj: Any, schemas: SerializationSchemas, input: DeserializationInput): PrivateKey {
        val bits = input.readObject(obj, schemas, ByteArray::class.java) as ByteArray
        return Crypto.decodePrivateKey(bits)
    }
}
 */

object RpcServerObservableSerializer : CustomSerializer.Implements<Observable<*>> (Observable::class.java){
    val observableContext = RPCServer.ObservableContext

    override val schemaForDocumentation: Schema
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override fun readObject(obj: Any, schemas: SerializationSchemas, input: DeserializationInput): Observable<*> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun writeDescribedObject(obj: Observable<*>, data: Data, type: Type, output: SerializationOutput) {
        val observableId = Trace.InvocationId.newInstance()



        output.writeInvocationId(observableId)
        val observableWithSubscription = ObservableSubscription(
                // We capture [observableContext] in the subscriber. Note that all synchronisation/kryo borrowing
                // must be done again within the subscriber
                subscription = observable.materialize().subscribe(
                        object : Subscriber<Notification<*>>() {
                            override fun onNext(observation: Notification<*>) {
                                if (!isUnsubscribed) {
                                    val message = RPCApi.ServerToClient.Observation(
                                            id = observableId,
                                            content = observation,
                                            deduplicationIdentity = observableContext.deduplicationIdentity
                                    )
                                    observableContext.sendMessage(message)
                                }
                            }

                            override fun onError(exception: Throwable) {
                                RpcServerObservableSerializer.log.error("onError called in materialize()d RPC Observable", exception)
                            }

                            override fun onCompleted() {
                            }
                        }
                )
        )
        observableContext.clientAddressToObservables.put(observableContext.clientAddress, observableId)
        observableContext.observableMap.put(observableId, observableWithSubscription)

    }


    private fun Output.writeInvocationId(id: Trace.InvocationId) {
        writeString(id.value)
        writeLong(id.timestamp.toEpochMilli())
    }
}
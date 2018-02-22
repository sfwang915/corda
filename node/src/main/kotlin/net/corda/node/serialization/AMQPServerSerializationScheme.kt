package net.corda.node.serialization

import net.corda.core.cordapp.Cordapp
import net.corda.core.serialization.SerializationContext
import net.corda.nodeapi.internal.serialization.CordaSerializationMagic
import net.corda.nodeapi.internal.serialization.amqp.AbstractAMQPSerializationScheme
import net.corda.nodeapi.internal.serialization.amqp.SerializerFactory

class AMQPServerSerializationScheme(cordapps: List<Cordapp> = emptyList()) : AbstractAMQPSerializationScheme(cordapps) {
    override fun rpcClientSerializerFactory(context: SerializationContext): SerializerFactory {
        throw UnsupportedOperationException()
    }

    override fun rpcServerSerializerFactory(context: SerializationContext): SerializerFactory {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun canDeserializeVersion(magic: CordaSerializationMagic, target: SerializationContext.UseCase): Boolean {
        return canDeserializeVersion(magic) &&
                (  target == SerializationContext.UseCase.P2P
                        || target == SerializationContext.UseCase.Storage
                        || target == SerializationContext.UseCase.RPCServer)
    }
}

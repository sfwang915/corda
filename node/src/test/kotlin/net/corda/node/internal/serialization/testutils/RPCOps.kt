package net.corda.node.internal.serialization.testutils

import net.corda.core.messaging.RPCOps

class TestRPCOps : RPCOps {
    override val protocolVersion: Int get() = -1
}
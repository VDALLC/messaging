<?php
namespace Vda\Messaging;

interface IMessageConsumer
{
    public function subscribe(Subscription $subscription);
    public function unsubscribe(Subscription $subscription);
    /**
     * Read message from broker
     *
     * @param int $timeout Seconds to wait for message, -1 to wait indefinitely
     * @return Message
     */
    public function receive($timeout = -1);
    public function ack($messageId);
}

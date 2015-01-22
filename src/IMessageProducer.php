<?php
namespace Vda\Messaging;

interface IMessageProducer
{
    /**
     * Send $message to the destination queue/topic
     *
     * The message posted to queue (/queue/<queue-name>) will be delivered to
     * only one consumer attached to it (if any). Use this to distribute tasks
     * between multiple workers.
     *
     * The message posted to topic (/topic/<topic-name>) will be delivered to
     * every consumer subscribed to it (if any). Use this to broadcast messages.
     *
     * @param string $destination
     * @param Message $message
     */
    public function send($destination, Message $message);
}

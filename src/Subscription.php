<?php
namespace Vda\Messaging;

class Subscription
{
    const ACK_AUTO = 0;
    const ACK_INDIVIDUAL = 1;
    const ACK_CUMULATIVE = 2;

    private $destination;
    private $id;
    private $durable;
    private $ackMode;

    public function __construct(
        $destination,
        $id = null,
        $isDurable = false,
        $ackMode = self::ACK_AUTO
    ) {
        $this->destination = $destination;
        $this->id = is_null($id) ? $destination : $id;
        $this->durable = $isDurable;
        $this->ackMode = $ackMode;
    }

    public function getDestination()
    {
        return $this->destination;
    }

    public function getId()
    {
        return $this->id;
    }

    public function isDurable()
    {
        return $this->durable;
    }

    public function getAckMode()
    {
        return $this->ackMode;
    }
}

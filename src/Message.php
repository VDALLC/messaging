<?php
namespace Vda\Messaging;

class Message
{
    private $id;
    private $body;
    private $persistent;
    private $priority;

    public function __construct($body, $id = null)
    {
        $this->body = $body;
        $this->id = $id;
    }

    public function getId()
    {
        return $this->id;
    }

    public function getBody()
    {
        return $this->body;
    }

    public function isPersistent()
    {
        return $this->persistent;
    }

    public function setPersistent($persistent)
    {
        $this->persistent = $persistent;
    }

    public function getPriority()
    {
        return $this->priority;
    }

    public function setPriority($priority)
    {
        $this->priority = $priority;
    }
}

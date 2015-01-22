<?php
namespace Vda\Messaging\Stomp;

class StompMessage
{
    private $command;
    private $headers;
    private $body;

    /**
     * @param string $body
     * @param array $headers
     * @param string $command
     */
    public function __construct($body, array $headers = array(), $command = 'SEND')
    {
        $this->body = $body;
        $this->headers = $headers;
        $this->command = $command;
    }

    public function getBody()
    {
        return $this->body;
    }

    public function setBody($body)
    {
        $this->body = $body;
    }

    public function getHeaders()
    {
        return $this->headers;
    }

    public function getHeader($name)
    {
        return isset($this->headers[$name]) ? $this->headers[$name] : null;
    }

    public function setHeader($name, $value)
    {
        $this->headers[$name] = $value;
    }

    public function unsetHeader($name)
    {
        if (array_key_exists($name, $this->headers)) {
            unset($this->headers[$name]);
        }
    }

    public function clearHeaders()
    {
        $this->headers = array();
    }

    public function getCommand()
    {
        return $this->command;
    }

    public function setCommand($command)
    {
        $this->command = $command;
    }
}

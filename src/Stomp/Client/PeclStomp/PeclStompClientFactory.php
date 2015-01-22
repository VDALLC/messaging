<?php
namespace Vda\Messaging\Stomp\Client\PeclStomp;

use \Vda\Messaging\Stomp\Client\IStompClientFactory;

class PeclStompClientFactory implements IStompClientFactory
{
    private $brokerAddress;
    private $username;
    private $password;

    public function __construct($connectionStr)
    {
        $url = parse_url($connectionStr);

        if ($url === false) {
            throw new \InvalidArgumentException("Unable to parse connection string");
        }

        if (empty($url['scheme'])) {
            $url['scheme'] = 'tcp';
        }

        $this->brokerAddress = "{$url['scheme']}://{$url['host']}";
        if (!empty($url['port'])) {
            $this->brokerAddress .= ":{$url['port']}";
        }

        if (!empty($url['user'])) {
            $this->username = $url['user'];
        }

        if (!empty($url['pass'])) {
            $this->password = $url['pass'];
        }
    }

    public function create($clientId = null)
    {
        return new PeclStompClient(
            $this->brokerAddress,
            $clientId,
            $this->username,
            $this->password
        );
    }
}

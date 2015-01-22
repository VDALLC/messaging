<?php
namespace Vda\Messaging\Stomp\Client;

interface IStompClientFactory
{
    /**
     * @param string $clientId Optional client identifier
     * @return IStompClient
     */
    public function create($clientId = null);
}

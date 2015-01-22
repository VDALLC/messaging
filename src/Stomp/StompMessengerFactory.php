<?php
namespace Vda\Messaging\Stomp;

use \Vda\Messaging\IMessengerFactory;
use \Vda\Messaging\Stomp\Client\IStompClientFactory;

class StompMessengerFactory implements IMessengerFactory
{
    /**
     * @var IStompClientFactory
     */
    private $clientFactory;

    public function __construct(IStompClientFactory $clientFactory)
    {
        $this->clientFactory = $clientFactory;
    }

    public function createMessenger($clientId)
    {
        return new StompMessenger($this->clientFactory->create($clientId));
    }
}

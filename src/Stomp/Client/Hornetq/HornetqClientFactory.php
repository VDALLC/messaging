<?php
namespace Vda\Messaging\Stomp\Client\Hornetq;

use \Vda\Messaging\Stomp\Client\IStompClientFactory;

class HornetqClientFactory implements IStompClientFactory
{
    /**
     * @var IStompClientFactory
     */
    private $backendFactory;

    public function __construct(IStompClientFactory $backendFactory)
    {
        $this->backendFactory = $backendFactory;
    }

    public function create($clientId = null)
    {
        return new HornetqClient($this->backendFactory->create($clientId));
    }
}

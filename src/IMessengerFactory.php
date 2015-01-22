<?php
namespace Vda\Messaging;

interface IMessengerFactory
{
    /**
     * @param string $clientId
     * @return IMessenger
     */
    public function createMessenger($clientId);
}

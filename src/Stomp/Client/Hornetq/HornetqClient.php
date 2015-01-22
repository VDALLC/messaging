<?php
namespace Vda\Messaging\Stomp\Client\Hornetq;

use Vda\Messaging\MessagingException;

use \Vda\Messaging\Message;
use \Vda\Messaging\Stomp\Client\IStompClient;
use \Vda\Messaging\Stomp\StompMessage;
use \Vda\Messaging\Subscription;

/**
 * Decorates IStompClient to incapsulate HornetQ specific protocol details
 */
class HornetqClient implements IStompClient
{
    /**
     * Actual backend to communicate stomp server
     *
     * @var IStompClient
     */
    private $stomp;

    public function __construct(IStompClient $stomp)
    {
        $this->stomp = $stomp;
    }

    public function connect(array $headers = array())
    {
        $headers['client-id'] = $this->getClientId();

        $this->stomp->connect($headers);
    }

    public function disconnect()
    {
        $this->stomp->disconnect();
    }

    public function isConnected()
    {
        return $this->stomp->isConnected();
    }

    public function send($destination, StompMessage $message)
    {
        $this->stomp->send($this->adjustDestination($destination), $message);
    }

    public function subscribe($destination, array $headers = array())
    {
        return $this->stomp->subscribe(
            $this->adjustDestination($destination),
            $headers
        );
    }

    public function unsubscribe($destination, array $headers = array())
    {
        return $this->stomp->unsubscribe(
            $this->adjustDestination($destination),
            $headers
        );
    }

    public function getClientId()
    {
        return $this->stomp->getClientId();
    }

    public function hasMessage()
    {
        return $this->stomp->hasFrame();
    }

    public function readMessage()
    {
        return $this->stomp->readMessage();
    }

    public function ack($messageId, array $headers = array())
    {
        $this->stomp->ack($messageId, $headers);
    }

    public function adaptMessage(Message $message, $command = 'SEND')
    {
        $frame = $this->stomp->adaptMessage($message, $command);

        if ($message->isPersistent()) {
            $frame->setHeader('persistent', 'true');
        }

        if (!is_null($message->getPriority())) {
            $frame->setHeader('priority', $message->getPriority());
        }

        return $frame;
    }

    public function adaptSubscription(Subscription $subscription, $command = 'SUBSCRIBE')
    {
        if ($subscription->isDurable() && is_null($this->getClientId())) {
            throw new MessagingException(
                'Client id must be set for durable subscriptions'
            );
        }

        $frame = $this->stomp->adaptSubscription($subscription, $command);

        if ($subscription->isDurable()) {
            $frame->setHeader('durable-subscriber-name', $subscription->getId());
        }

        return $frame;
    }

    public function getReadTimeout()
    {
        return $this->stomp->getReadTimeout();
    }

    public function setReadTimeout($seconds, $microseconds = 0)
    {
        $this->stomp->setReadTimeout($seconds, $microseconds);
    }

    public function begin($transactionId, array $headers = array())
    {
        return $this->stomp->begin($transactionId, $headers);
    }

    public function commit($transactionId, array $headers = array())
    {
        return $this->stomp->commit($transactionId, $headers);
    }

    public function abort($transactionId, array $headers = array())
    {
        return $this->stomp->abort($transactionId, $headers);
    }

    public function getSessionId()
    {
        return $this->stomp->getSessionId();
    }

    private function adjustDestination($destination)
    {
        return 'jms.' . str_replace('/', '.', trim($destination, '/'));
    }
}
